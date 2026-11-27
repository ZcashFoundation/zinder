//! Reusable runtime entrypoints and CLI parsing for `zinder-query`.

pub mod config;
mod grpc_healthcheck;

use std::{
    ffi::OsString,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;
use zinder_core::wire::encode_zinder_native_chain_name;
use zinder_runtime::{
    OpsHealthcheckArgs, OpsServerError, Readiness, RuntimeService, StartupPhase, SupervisedRuntime,
    SupervisedTasks, TracingSubscriberError, host_cpu_meets_compiled_baseline,
    host_cpu_meets_pasta_backend_baseline, install_metrics_recorder_for_service,
    install_tracing_subscriber, section_defaults::default_ops_listen_addr, spawn_ops_endpoint_for,
};
use zinder_source::{DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS, ZebraJsonRpcSource};

use crate::{
    AdmittedIngestControl, ForwardedReadLimit, OrderedBroadcaster, WalletEndpointMetadata,
    WalletQueryApi, WalletQueryGrpcAdapter, WalletServingPairConfig, WalletServingPairPublisher,
    WalletServingQuery, WalletServingReadiness, spawn_wallet_ingest_control_readiness_probe,
};

pub use config::{
    QueryConfig, QueryConfigError, QueryConfigOverrides, load_query_config,
    load_query_config_with_env, query_config_toml,
};

/// Command-line interface for the native `WalletQuery` gRPC server.
#[derive(Debug, Parser)]
#[command(name = "zinder-query")]
#[command(about = "Zinder native WalletQuery gRPC server")]
#[command(version)]
pub struct Cli {
    /// TOML configuration file loaded before environment variables and CLI overrides.
    #[arg(long = "config", global = true)]
    pub config_path: Option<PathBuf>,
    /// Print the resolved configuration without opening storage or binding.
    #[arg(long = "print-config", global = true)]
    pub print_config: bool,
    /// Network name, such as zcash-regtest.
    #[arg(long)]
    pub network: Option<String>,
    /// Canonical primary path replicated only through an immutable secondary.
    #[arg(long = "canonical-primary-path")]
    pub canonical_primary_path: Option<PathBuf>,
    /// Root containing this process's canonical secondary generations.
    #[arg(long = "canonical-secondary-root")]
    pub canonical_secondary_root: Option<PathBuf>,
    /// Canonical raw-blob retention expected from the writer.
    #[arg(
        long = "raw-blob-policy",
        value_parser = ["none", "all"]
    )]
    pub raw_blob_policy: Option<String>,
    /// Wallet primary path replicated only through an immutable secondary.
    #[arg(long = "wallet-primary-path")]
    pub wallet_primary_path: Option<PathBuf>,
    /// Root containing this process's wallet secondary generations.
    #[arg(long = "wallet-secondary-root")]
    pub wallet_secondary_root: Option<PathBuf>,
    /// Private `zinder-ingest` control gRPC endpoint.
    #[arg(long = "ingest-control-addr")]
    pub ingest_control_addr: Option<String>,
    /// File containing the shared-secret bearer token used by `IngestControl`.
    #[arg(long = "ingest-control-token-path")]
    pub ingest_control_token_path: Option<PathBuf>,
    /// Native `WalletQuery` gRPC listen address.
    #[arg(long = "listen-addr")]
    pub listen_addr: Option<SocketAddr>,
    /// Exact canonical replacement-depth identity expected from the writer.
    #[arg(long = "reorg-window-blocks")]
    pub reorg_window_blocks: Option<u32>,
    /// Operational HTTP endpoint listen address for health and metrics.
    #[arg(long = "ops-listen-addr")]
    pub ops_listen_addr: Option<SocketAddr>,
    /// Node JSON-RPC address used for transaction broadcast. Omit to disable broadcast.
    #[arg(long = "node-json-rpc-addr")]
    pub node_json_rpc_addr: Option<String>,
    /// Optional operational subcommand.
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Subcommands supported by `zinder-query`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Probe the operational endpoint once and exit 0 only when it answers
    /// HTTP 200. Reads only `[ops] listen_addr` from the `--config` file.
    Healthcheck(OpsHealthcheckArgs),
    /// Probe the gRPC listener. Exit 0 if serving, 3 if it reports non-serving,
    /// or 1 if the health check fails.
    GrpcHealthcheck {
        /// Plaintext listener socket. An unspecified IP probes loopback.
        addr: SocketAddr,
    },
}

/// Runs the standalone `zinder-query` CLI from process arguments.
pub async fn main_from_env(binary_name: &str) -> ExitCode {
    dispatch_cli(binary_name, Cli::try_parse(), true).await
}

/// Runs this role as the process entry point, owning its tracing subscriber.
pub async fn run_process_from_args<I, T>(binary_name: &str, args: I) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    dispatch_cli(binary_name, Cli::try_parse_from(args), true).await
}

/// Runs this role from explicit arguments using the caller's tracing subscriber.
pub async fn run_from_args<I, T>(binary_name: &str, args: I) -> ExitCode
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    dispatch_cli(binary_name, Cli::try_parse_from(args), false).await
}

async fn dispatch_cli(
    binary_name: &str,
    parsed: Result<Cli, clap::Error>,
    owns_process_tracing: bool,
) -> ExitCode {
    let cli = match parsed {
        Ok(cli) => cli,
        Err(error) => return print_clap_error(&error),
    };
    if let Some(command) = &cli.command {
        return match command {
            Command::Healthcheck(healthcheck) => {
                run_healthcheck(binary_name, healthcheck, cli.config_path.as_deref())
            }
            Command::GrpcHealthcheck { addr } => grpc_healthcheck::run(*addr).await,
        };
    }
    if owns_process_tracing {
        run_with_process_tracing(binary_name, cli).await
    } else {
        run_parsed_cli(cli).await
    }
}

/// Supervises the native `WalletQuery` server inside a parent runtime such as `zinder serve`.
///
/// The caller owns the operational HTTP endpoint, metrics recorder, CPU checks,
/// and terminating-signal handling. `readiness` receives readiness updates and
/// `cancel` triggers bounded shutdown.
pub async fn serve_supervised(
    mut query_config: QueryConfig,
    readiness: Readiness,
    cancel: CancellationToken,
) -> Result<(), QueryConfigError> {
    query_config.ops_listen_addr = None;
    let runtime = SupervisedRuntime::new(RuntimeService::Query);
    let shutdown = runtime.shutdown_request();
    let cancel_token = cancel.clone();
    let cancel_bridge = tokio::spawn(async move {
        cancel_token.cancelled().await;
        shutdown.request();
    });
    let outcome = serve_query(query_config, runtime, readiness, cancel).await;
    cancel_bridge.abort();
    outcome
}

async fn run_with_process_tracing(binary_name: &str, cli: Cli) -> ExitCode {
    if cli.print_config {
        if let Err(error) = install_tracing_subscriber() {
            return emit_tracing_subscriber_error(binary_name, &error);
        }
        return run_parsed_cli(cli).await;
    }
    let tracing = match zinder_runtime::install_runtime_tracing_subscriber(
        RuntimeService::Query,
        cli.config_path.as_deref(),
    ) {
        Ok(tracing) => tracing,
        Err(error) => return emit_tracing_subscriber_error(binary_name, &error),
    };
    let exit = run_parsed_cli(cli).await;
    tracing.shutdown().await;
    exit
}

async fn run_parsed_cli(cli: Cli) -> ExitCode {
    if !host_cpu_meets_compiled_baseline() {
        return ExitCode::FAILURE;
    }
    if !host_cpu_meets_pasta_backend_baseline() {
        return ExitCode::FAILURE;
    }
    if cli.print_config {
        return print_config(cli);
    }
    match run_query(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => emit_runtime_error(&error),
    }
}

#[allow(
    clippy::print_stdout,
    reason = "--print-config is a structured TOML data dump, not a log event"
)]
fn print_config(cli: Cli) -> ExitCode {
    let config_path = cli.config_path.clone();
    let rendered = config::load_query_config(config_path, cli.into())
        .and_then(|query_config| config::query_config_toml(&query_config));
    match rendered {
        Ok(rendered) => {
            println!("{rendered}");
            ExitCode::SUCCESS
        }
        Err(error) => emit_runtime_error(&error),
    }
}

async fn run_query(cli: Cli) -> Result<(), QueryConfigError> {
    let load_config_phase = StartupPhase::LoadConfig.start();
    let config_path = cli.config_path.clone();
    let query_config = match config::load_query_config(config_path, cli.into()) {
        Ok(query_config) => {
            load_config_phase.complete();
            query_config
        }
        Err(error) => {
            load_config_phase.fail(&error);
            return Err(error);
        }
    };

    if query_config.ops_listen_addr.is_some() {
        install_metrics_recorder_for_service(
            RuntimeService::Query,
            env!("CARGO_PKG_VERSION"),
            encode_zinder_native_chain_name(query_config.network),
        )
        .map_err(OpsServerError::from)?;
    }

    let runtime = SupervisedRuntime::new(RuntimeService::Query);
    let _signal_handle = runtime.request_shutdown_on_terminating_signal();
    let cancel = runtime.stopping();
    serve_query(query_config, runtime, Readiness::default(), cancel).await
}

#[allow(
    clippy::too_many_lines,
    reason = "the linear startup sequence keeps operator-visible failure ordering auditable"
)]
async fn serve_query(
    query_config: QueryConfig,
    runtime: SupervisedRuntime,
    readiness: Readiness,
    startup_cancel: CancellationToken,
) -> Result<(), QueryConfigError> {
    let serving_readiness = WalletServingReadiness::with_node_and_ingest_control(readiness.clone());
    let admit_ingest_control_phase = StartupPhase::AdmitIngestControl.start();
    let ingest_control = tokio::select! {
        outcome = AdmittedIngestControl::connect(
            &query_config.ingest_control_addr,
            query_config.ingest_control_bearer_token.as_ref(),
            query_config.network,
        ) => match outcome {
            Ok(ingest_control) => {
                admit_ingest_control_phase.complete();
                ingest_control
            }
            Err(error) => {
                admit_ingest_control_phase.fail(&error);
                return Err(error.into());
            }
        },
        () = startup_cancel.cancelled() => {
            admit_ingest_control_phase.complete();
            return Ok(());
        }
    };
    let node_broadcast =
        build_node_broadcast(query_config.node.as_ref(), &query_config.node_fallbacks)?;

    let open_storage_phase = StartupPhase::OpenStorage.start();
    let pair_config = WalletServingPairConfig {
        canonical_primary_path: query_config.storage.path.clone(),
        canonical_secondary_root: query_config.storage.secondary_path.clone(),
        wallet_primary_path: query_config.wallet_primary_path.clone(),
        wallet_secondary_root: query_config.wallet_secondary_root.clone(),
        network: query_config.network,
        canonical_requirements: query_config.canonical_requirements(),
        canonical_reorg_policy: query_config.canonical_reorg_policy,
        canonical_resource_budget: query_config.storage.canonical_rocksdb_budget,
        wallet_resource_budget: query_config.wallet_rocksdb_budget,
        catchup_interval: query_config.storage.secondary_catchup_interval,
        convergence_timeout: query_config.storage.initial_catchup_timeout,
        convergence_attempts: query_config.pair_convergence_attempts,
        replica_lag_threshold_chain_epochs: query_config
            .storage
            .secondary_replica_lag_threshold_chain_epochs,
        serving_pair_staleness_ceiling: query_config.storage.serving_pair_staleness_ceiling,
        epoch_pin_retention: query_config.epoch_pin_retention,
    };
    let (pair_publisher, pair_slot) = tokio::select! {
        outcome = WalletServingPairPublisher::bootstrap_with_admitted_ingest_control(
            pair_config,
            serving_readiness.clone(),
            &ingest_control,
        ) => outcome?,
        () = startup_cancel.cancelled() => {
            open_storage_phase.complete();
            return Ok(());
        }
    };
    let visible_height = pair_slot
        .capture_ephemeral()
        .canonical_fence()
        .visible_tip()
        .height
        .value();
    open_storage_phase.complete();

    let query =
        WalletServingQuery::from_admitted_native_serving_pair(pair_slot, ingest_control.clone())
            .with_node_broadcast(node_broadcast)
            .with_verified_cold_transactions(ForwardedReadLimit::new(query_config.forwarded_reads))
            .with_tree_state_replay_wait(query_config.tree_state_replay_wait)
            .with_tree_state_replay_waiters_per_slot(
                query_config.tree_state_replay_waiters_per_slot,
            );
    let endpoint_metadata = WalletEndpointMetadata {
        network: encode_zinder_native_chain_name(query_config.network).to_owned(),
        service_version: env!("CARGO_PKG_VERSION").to_owned(),
        schema_version: u32::from(zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION.value()),
        reorg_window_blocks: query_config.canonical_reorg_policy.reorg_window_blocks(),
        ..WalletEndpointMetadata::default()
    };
    let native_endpoint_capabilities = query.native_endpoint_capabilities().clone();
    let advertised_capabilities = native_endpoint_capabilities.shared_identifiers();
    let start_api_phase = StartupPhase::StartApi.start();
    let ops_handle = spawn_ops_endpoint_for(
        RuntimeService::Query,
        query_config.ops_listen_addr,
        env!("CARGO_PKG_VERSION"),
        encode_zinder_native_chain_name(query_config.network),
        readiness.clone(),
        advertised_capabilities,
    )
    .await?;
    let cancel = runtime.stopping();
    let node_readiness_handle = query.spawn_node_readiness_probe(
        serving_readiness.clone(),
        query_config
            .node
            .as_ref()
            .and_then(|node| node.health.as_ref())
            .map_or_else(
                || std::time::Duration::from_millis(DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS),
                |health| health.poll_interval,
            ),
        cancel.clone(),
    );
    let grpc_adapter = WalletQueryGrpcAdapter::new(query, endpoint_metadata)
        .with_traffic_readiness(readiness.clone())
        .with_shutdown(cancel.clone());

    // Health and serving-pair refresh observe the same ingest control plane, so
    // they intentionally share one configured freshness cadence.
    let ingest_control_health_poll_interval = query_config.storage.secondary_catchup_interval;
    let ingest_control_readiness_handle = spawn_wallet_ingest_control_readiness_probe(
        ingest_control,
        serving_readiness.clone(),
        ingest_control_health_poll_interval,
        cancel.clone(),
    );
    let publisher_handle = pair_publisher.spawn(cancel, runtime.serving_drained());
    let router = runtime.public_grpc_router_with_transport(
        (
            grpc_adapter.into_server(),
            zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET,
        ),
        query_config.client_address_source,
        &readiness,
        |server| {
            server
                .with_proxy_protocol(query_config.proxy_protocol.clone())
                .layer(
                    zinder_runtime::WalletRpcMetricsLayer::WalletQuery.with_range_admission(
                        query_config.range_admission,
                        query_config.client_address_source,
                    ),
                )
        },
    )?;

    tracing::info!(
        target: "zinder::query",
        event = "query_started",
        network = encode_zinder_native_chain_name(query_config.network),
        listen_addr = %query_config.listen_addr,
        visible_height,
        "native WalletQuery gRPC server started"
    );
    start_api_phase.complete();
    StartupPhase::Ready.start().complete();
    let server =
        router.serve_with_shutdown(query_config.listen_addr, runtime.grpc_server_shutdown());
    runtime
        .supervise(
            server,
            SupervisedTasks::new()
                .with_fallible("serving-pair publisher", publisher_handle)
                .with("node-readiness probe", node_readiness_handle)
                .with(
                    "ingest-control readiness probe",
                    ingest_control_readiness_handle,
                )
                .with_operations_endpoint(ops_handle),
            || serving_readiness.publish_shutting_down(),
        )
        .await?;
    Ok(())
}

/// Builds the broadcast endpoints from `[node]` and `[node_fallbacks.N]`
/// without contacting any node, or `None` when no node is configured.
fn build_node_broadcast(
    node: Option<&zinder_source::NodeTarget>,
    node_fallbacks: &[zinder_source::NodeTarget],
) -> Result<Option<OrderedBroadcaster<ZebraJsonRpcSource>>, QueryConfigError> {
    let Some(node) = node else {
        tracing::info!(
            target: "zinder::query",
            event = "transaction_broadcast_disabled",
            "transaction broadcast disabled because [node] is not configured"
        );
        return Ok(None);
    };
    let nodes = OrderedBroadcaster::connect(node, node_fallbacks)
        .map_err(|error| QueryConfigError::Source(Box::new(error)))?;
    tracing::info!(
        target: "zinder::query",
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
fn run_healthcheck(
    binary_name: &str,
    healthcheck: &OpsHealthcheckArgs,
    config_path: Option<&Path>,
) -> ExitCode {
    match healthcheck.run(config_path, default_ops_listen_addr(RuntimeService::Query)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{binary_name}: healthcheck failed: {error}");
            ExitCode::FAILURE
        }
    }
}

#[allow(
    clippy::print_stderr,
    reason = "no tracing subscriber is installed, so stderr is the only channel left"
)]
fn emit_tracing_subscriber_error(binary_name: &str, error: &TracingSubscriberError) -> ExitCode {
    eprintln!("{binary_name}: {error}");
    ExitCode::FAILURE
}

fn print_clap_error(error: &clap::Error) -> ExitCode {
    let exit_code = if error.exit_code() == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(u8::try_from(error.exit_code()).unwrap_or(2))
    };
    let _ = error.print();
    exit_code
}

fn emit_runtime_error(error: &QueryConfigError) -> ExitCode {
    tracing::error!(
        target: "zinder::query",
        event = "query_run_failed",
        error = %error,
        "native query run failed"
    );
    ExitCode::FAILURE
}

impl From<Cli> for QueryConfigOverrides {
    fn from(cli: Cli) -> Self {
        Self {
            network: cli.network,
            canonical_primary_path: cli.canonical_primary_path,
            canonical_secondary_root: cli.canonical_secondary_root,
            raw_blob_policy: cli.raw_blob_policy,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use zinder_core::Network;
    use zinder_source::{DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES, NodeAuth, NodeTarget};

    fn node_target(json_rpc_addr: &str, node_auth: NodeAuth) -> NodeTarget {
        NodeTarget::new(
            Network::ZcashRegtest,
            json_rpc_addr.to_owned(),
            node_auth,
            Duration::from_secs(5),
            DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES,
        )
    }

    /// The node writes its cookie file when it starts, so a query that starts
    /// first keeps an unverified broadcast endpoint instead of exiting.
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
            Err(QueryConfigError::Source(_))
        ));
    }

    #[test]
    fn a_malformed_node_address_fails_startup() {
        let node = node_target("not a node address", NodeAuth::None);

        assert!(matches!(
            build_node_broadcast(Some(&node), &[]),
            Err(QueryConfigError::Source(_))
        ));
    }
}
