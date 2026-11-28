//! Reusable runtime entrypoints and CLI parsing for `zinder-compat-lightwalletd`.

mod compact_block_serving;
pub mod config;
mod listener;
mod supervision;

use std::{
    ffi::OsString,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
};

use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;
use zinder_core::wire::encode_zinder_native_chain_name;
use zinder_query::{
    AdmittedIngestControl, CanonicalReader as _, DEFAULT_TREE_STATE_REPLAY_WAIT,
    DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT, ForwardedReadLimit, OrderedBroadcaster,
    WalletServingPairConfig, WalletServingPairPublisher, WalletServingQuery,
    WalletServingReadiness, spawn_wallet_ingest_control_readiness_probe,
};
use zinder_runtime::{
    OpsHealthcheckArgs, OpsServerError, Readiness, RuntimeService, StartupPhase, SupervisedRuntime,
    TracingSubscriberError, host_cpu_meets_compiled_baseline,
    host_cpu_meets_pasta_backend_baseline, install_metrics_recorder_for_service,
    install_tracing_subscriber, section_defaults::default_ops_listen_addr, spawn_ops_endpoint_for,
};
use zinder_source::{DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS, NodeTarget, ZebraJsonRpcSource};

use crate::{
    IngestControlMempoolSurface, LightwalletdGrpcAdapter, LightwalletdPing, NodeTipWitness,
    TipWait, spawn_ingest_control_tip_change_publisher, spawn_node_tip_witness,
};

pub use compact_block_serving::CompactBlockServingError;
pub use config::{
    CompatServing, LightwalletdConfig, LightwalletdConfigError, LightwalletdConfigOverrides,
    lightwalletd_config_toml, load_lightwalletd_config, load_lightwalletd_config_with_env,
};
use supervision::{CompactBlocksServingTasks, WalletServingTasks};

/// Command-line interface for the `zinder-compat-lightwalletd` gRPC server.
#[derive(Parser)]
#[command(name = "zinder-compat-lightwalletd")]
#[command(about = "Zinder lightwalletd-compatible gRPC server")]
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
    /// Compatibility capability: wallet or compact-blocks.
    #[arg(long)]
    pub serving: Option<CompatServing>,
    /// Canonical primary path replicated only through an immutable secondary.
    #[arg(long = "canonical-primary-path")]
    pub canonical_primary_path: Option<PathBuf>,
    /// Root for the canonical secondary generations used by this process.
    #[arg(long = "canonical-secondary-root")]
    pub canonical_secondary_root: Option<PathBuf>,
    /// Wallet primary path replicated only through an immutable secondary.
    #[arg(long = "wallet-primary-path")]
    pub wallet_primary_path: Option<PathBuf>,
    /// Root containing this process's two wallet secondary generations.
    #[arg(long = "wallet-secondary-root")]
    pub wallet_secondary_root: Option<PathBuf>,
    /// Private `zinder-ingest` control `gRPC` endpoint.
    #[arg(long = "ingest-control-addr")]
    pub ingest_control_addr: Option<String>,
    /// Path to a file containing the shared-secret bearer token used by the
    /// `IngestControl` writer. Required when the writer enforces auth.
    #[arg(long = "ingest-control-token-path")]
    pub ingest_control_token_path: Option<PathBuf>,
    /// Lightwalletd-compatible `gRPC` listen address, such as `127.0.0.1:9067`.
    #[arg(long = "listen-addr")]
    pub listen_addr: Option<SocketAddr>,
    /// Exact canonical replacement-depth identity expected from the writer.
    #[arg(long = "reorg-window-blocks")]
    pub reorg_window_blocks: Option<u32>,
    /// Operational HTTP endpoint listen address for `/healthz`, `/readyz`, `/metrics`.
    #[arg(long = "ops-listen-addr")]
    pub ops_listen_addr: Option<SocketAddr>,
    /// Node JSON-RPC address used for transaction broadcast and the recommended fee. Omit to disable both.
    #[arg(long = "node-json-rpc-addr")]
    pub node_json_rpc_addr: Option<String>,
    /// Answer the `Ping` load-testing RPC, as `lightwalletd --ping-very-insecure` does.
    #[arg(long = "ping-very-insecure")]
    pub ping_very_insecure: bool,
    /// Optional operational subcommand.
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Subcommands supported by `zinder-compat-lightwalletd`.
#[derive(Subcommand)]
pub enum Command {
    /// Probe the operational endpoint once and exit 0 only when it answers
    /// HTTP 200. Reads only `[ops] listen_addr` from the `--config` file.
    Healthcheck(OpsHealthcheckArgs),
}

/// Runs the standalone `zinder-compat-lightwalletd` CLI from process arguments.
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
    if let Some(Command::Healthcheck(healthcheck)) = &cli.command {
        return run_healthcheck(binary_name, healthcheck, cli.config_path.as_deref());
    }
    if owns_process_tracing {
        run_with_process_tracing(binary_name, cli).await
    } else {
        run_parsed_cli(cli).await
    }
}

/// Supervises the lightwalletd compatibility adapter inside a parent runtime.
///
/// The caller owns the operational HTTP endpoint, metrics recorder, CPU checks,
/// and terminating-signal handling. `readiness` receives readiness updates and
/// `cancel` triggers bounded shutdown.
pub async fn serve_supervised(
    mut lightwalletd_config: LightwalletdConfig,
    readiness: Readiness,
    cancel: CancellationToken,
) -> Result<(), LightwalletdConfigError> {
    lightwalletd_config.ops_listen_addr = None;
    let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd);
    let shutdown = runtime.shutdown_request();
    let cancel_token = cancel.clone();
    let cancel_bridge = tokio::spawn(async move {
        cancel_token.cancelled().await;
        shutdown.request();
    });
    let outcome = execute_lightwalletd(lightwalletd_config, runtime, readiness, false).await;
    cancel_bridge.abort();
    outcome
}

fn cpu_meets_baseline() -> bool {
    host_cpu_meets_compiled_baseline() && host_cpu_meets_pasta_backend_baseline()
}

async fn run_with_process_tracing(binary_name: &str, cli: Cli) -> ExitCode {
    if cli.print_config {
        if let Err(error) = install_tracing_subscriber() {
            return emit_tracing_subscriber_error(binary_name, &error);
        }
        return run_parsed_cli(cli).await;
    }
    let tracing = match zinder_runtime::install_runtime_tracing_subscriber(
        RuntimeService::CompatLightwalletd,
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
    if !cpu_meets_baseline() {
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
fn print_rendered_config(render_outcome: Result<String, LightwalletdConfigError>) -> ExitCode {
    match render_outcome {
        Ok(rendered_toml) => {
            println!("{rendered_toml}");
            ExitCode::SUCCESS
        }
        Err(error) => emit_runtime_error(&error),
    }
}

fn run_print_config(cli: Cli) -> ExitCode {
    let config_path = cli.config_path.clone();
    let overrides = cli.into();
    let render_outcome = load_lightwalletd_config(config_path, overrides)
        .and_then(|cfg| lightwalletd_config_toml(&cfg));
    print_rendered_config(render_outcome)
}

async fn run_runtime(cli: Cli) -> ExitCode {
    match run_lightwalletd(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => emit_runtime_error(&error),
    }
}

fn load_runtime_config<T>(
    load: impl FnOnce() -> Result<T, LightwalletdConfigError>,
) -> Result<T, LightwalletdConfigError> {
    let load_config_phase = StartupPhase::LoadConfig.start();
    match load() {
        Ok(cfg) => {
            load_config_phase.complete();
            Ok(cfg)
        }
        Err(error) => {
            load_config_phase.fail(&error);
            Err(error)
        }
    }
}

async fn run_lightwalletd(cli: Cli) -> Result<(), LightwalletdConfigError> {
    let config_path = cli.config_path.clone();
    let lightwalletd_config =
        load_runtime_config(|| load_lightwalletd_config(config_path, cli.into()))?;
    if lightwalletd_config.ops_listen_addr.is_some() {
        install_metrics_recorder_for_service(
            RuntimeService::CompatLightwalletd,
            env!("CARGO_PKG_VERSION"),
            encode_zinder_native_chain_name(lightwalletd_config.network),
        )
        .map_err(OpsServerError::from)?;
    }
    let runtime = SupervisedRuntime::new(RuntimeService::CompatLightwalletd);
    let readiness = Readiness::default();
    execute_lightwalletd(lightwalletd_config, runtime, readiness, true).await
}

async fn execute_lightwalletd(
    lightwalletd_config: LightwalletdConfig,
    runtime: SupervisedRuntime,
    readiness: Readiness,
    handle_signals: bool,
) -> Result<(), LightwalletdConfigError> {
    tracing::info!(
        target: "zinder::compat_lightwalletd",
        event = "compat_config_resolved",
        requested_serving = lightwalletd_config.serving.as_str(),
        "resolved compatibility serving configuration"
    );
    if lightwalletd_config.serving != CompatServing::Wallet {
        return run_canonical(lightwalletd_config, runtime, readiness, handle_signals).await;
    }
    execute_wallet(lightwalletd_config, runtime, readiness, handle_signals).await
}

#[allow(
    clippy::too_many_lines,
    reason = "lightwalletd bootstrap composes readiness, storage, and serving in startup order"
)]
async fn execute_wallet(
    lightwalletd_config: LightwalletdConfig,
    runtime: SupervisedRuntime,
    readiness: Readiness,
    handle_signals: bool,
) -> Result<(), LightwalletdConfigError> {
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
                canonical_requirements: lightwalletd_config.canonical_requirements(),
                canonical_reorg_policy: lightwalletd_config.canonical_reorg_policy,
                canonical_resource_budget: lightwalletd_config.storage.canonical_rocksdb_budget,
                wallet_resource_budget: wallet_rocksdb_budget,
                catchup_interval: lightwalletd_config.storage.secondary_catchup_interval,
                convergence_timeout: lightwalletd_config.storage.initial_catchup_timeout,
                convergence_attempts: lightwalletd_config.pair_convergence_attempts,
                replica_lag_threshold_chain_epochs: lightwalletd_config
                    .storage
                    .secondary_replica_lag_threshold_chain_epochs,
                epoch_pin_retention: zinder_query::EpochPinRetention::default(),
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
            .capture_ephemeral()
            .canonical_fence()
            .visible_tip()
            .height
            .value(),
    );
    open_storage_phase.complete();

    let tree_state_replay_wait = lightwalletd_config
        .tree_state_replay_wait
        .unwrap_or(DEFAULT_TREE_STATE_REPLAY_WAIT);
    let tree_state_replay_waiters_per_slot = lightwalletd_config
        .tree_state_replay_waiters_per_slot
        .unwrap_or(DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT);
    let forwarded_reads =
        ForwardedReadLimit::new(lightwalletd_config.forwarded_reads.unwrap_or_default());
    let wallet_query = WalletServingQuery::from_serving_pair_slot(serving_pair_slot)
        .with_node_broadcast(node_broadcast)
        .with_verified_cold_transactions(forwarded_reads)
        .with_tree_state_replay_wait(tree_state_replay_wait)
        .with_tree_state_replay_waiters_per_slot(tree_state_replay_waiters_per_slot);
    let grpc_adapter =
        LightwalletdGrpcAdapter::from_admitted_compatibility_query(wallet_query.clone())?;
    let (incoming, listen_addr) = listener::bind(lightwalletd_config.listen_addr)?;
    let cancel = runtime.stopping();
    let (node_tip_witness, node_tip_witness_handle) =
        spawn_configured_node_tip_witness(&lightwalletd_config, &cancel);
    let grpc_adapter = grpc_adapter
        .with_max_concurrent_block_range_streams(
            lightwalletd_config.max_concurrent_block_range_streams,
        )
        .with_stale_tip_limit(lightwalletd_config.stale_tip_limit)
        .with_tip_wait(configured_tip_wait(&lightwalletd_config, &cancel))
        .with_node_tip_witness(node_tip_witness)
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
    let _signal_handle = handle_signals.then(|| runtime.request_shutdown_on_terminating_signal());
    let node_readiness_handle = wallet_query.spawn_node_readiness_probe(
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
    drop(wallet_query);
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
        tip_wait_ms = lightwalletd_config.tip_wait.as_millis(),
        tip_wait_max_blocks = lightwalletd_config.tip_wait_max_blocks.get(),
        tip_wait_max_waiters = lightwalletd_config.tip_wait_max_waiters.get(),
        tree_state_replay_wait_ms = tree_state_replay_wait.as_millis(),
        tree_state_replay_waiters_per_slot = tree_state_replay_waiters_per_slot.get(),
        ping_very_insecure = lightwalletd_config.ping_very_insecure,
        "lightwalletd-compatible gRPC server started"
    );

    let server = listener::serve(
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
    lightwalletd_config: &LightwalletdConfig,
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
    LightWallet {
        source: Box<ZebraJsonRpcSource>,
        ingest: AdmittedIngestControl,
        poll_interval: std::time::Duration,
    },
}

impl CanonicalComposition {
    async fn admit(config: &LightwalletdConfig) -> Result<Self, LightwalletdConfigError> {
        match config.serving {
            CompatServing::LightWallet => {
                let target = config.broadcaster.as_ref().ok_or_else(|| {
                    zinder_runtime::ConfigError::invalid("light-wallet serving requires [node]")
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
                Ok(Self::LightWallet {
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
    lightwalletd_config: LightwalletdConfig,
    runtime: SupervisedRuntime,
    readiness: Readiness,
    handle_signals: bool,
) -> Result<(), LightwalletdConfigError> {
    let composition = CanonicalComposition::admit(&lightwalletd_config).await?;
    let serving_readiness = match &composition {
        CanonicalComposition::LightWallet { .. } => {
            compact_block_serving::CompactServingReadiness::for_light_wallet(readiness.clone())
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
            requirements: lightwalletd_config.canonical_requirements(),
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
    let (incoming, listen_addr) = listener::bind(lightwalletd_config.listen_addr)?;
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
    let _signal_handle = handle_signals.then(|| runtime.request_shutdown_on_terminating_signal());
    let tasks = CompactBlocksServingTasks {
        compact_block_publisher: publisher.spawn(cancel.clone(), runtime.serving_drained()),
        node_tip_witness: node_tip_witness_handle,
        operations: ops_handle,
    }
    .supervised();
    let (server, tasks) = match composition {
        CanonicalComposition::LightWallet {
            source,
            ingest,
            poll_interval,
        } => {
            let adapter = LightwalletdGrpcAdapter::for_light_wallet(
                slot,
                *source,
                lightwalletd_config
                    .tree_state_replay_wait
                    .unwrap_or(DEFAULT_TREE_STATE_REPLAY_WAIT),
                lightwalletd_config
                    .tree_state_replay_waiters_per_slot
                    .unwrap_or(DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT),
                ForwardedReadLimit::new(lightwalletd_config.forwarded_reads.unwrap_or_default()),
            )
            .with_max_concurrent_block_range_streams(
                lightwalletd_config.max_concurrent_block_range_streams,
            )
            .with_stale_tip_limit(lightwalletd_config.stale_tip_limit)
            .with_tip_wait(configured_tip_wait(&lightwalletd_config, &cancel))
            .with_node_tip_witness(node_tip_witness)
            .with_ping(configured_ping(&lightwalletd_config, &cancel));
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
                listener::serve(
                    &runtime,
                    adapter.into_server(),
                    incoming,
                    &lightwalletd_config,
                    &readiness,
                )?,
                tasks
                    .with("light-wallet node probe", node_probe)
                    .with("light-wallet ingest probe", ingest_probe)
                    .with("tip-change publisher", tip_publisher),
            )
        }
        CanonicalComposition::CompactBlocks => {
            let grpc_adapter = crate::CompactBlocksGrpcAdapter::new(slot)
                .with_max_concurrent_block_range_streams(
                    lightwalletd_config.max_concurrent_block_range_streams,
                )
                .with_stale_tip_limit(lightwalletd_config.stale_tip_limit)
                .with_tip_wait(configured_tip_wait(&lightwalletd_config, &cancel))
                .with_node_tip_witness(node_tip_witness)
                .with_ping(configured_ping(&lightwalletd_config, &cancel));
            (
                listener::serve(
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
        tip_wait_ms = lightwalletd_config.tip_wait.as_millis(),
        tip_wait_max_blocks = lightwalletd_config.tip_wait_max_blocks.get(),
        tip_wait_max_waiters = lightwalletd_config.tip_wait_max_waiters.get(),
        ping_very_insecure = lightwalletd_config.ping_very_insecure,
        "lightwalletd-compatible gRPC server started"
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
fn run_healthcheck(
    binary_name: &str,
    healthcheck: &OpsHealthcheckArgs,
    config_path: Option<&Path>,
) -> ExitCode {
    match healthcheck.run(
        config_path,
        default_ops_listen_addr(RuntimeService::CompatLightwalletd),
    ) {
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
    lightwalletd_config: &LightwalletdConfig,
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

/// Builds the tip wait from configuration, ended by a graceful shutdown. A
/// zero wait disables it.
fn configured_tip_wait(
    lightwalletd_config: &LightwalletdConfig,
    shutdown: &CancellationToken,
) -> TipWait {
    if lightwalletd_config.tip_wait.is_zero() {
        return TipWait::disabled();
    }
    TipWait::new(
        lightwalletd_config.tip_wait,
        lightwalletd_config.tip_wait_max_blocks,
        lightwalletd_config.tip_wait_max_waiters,
    )
    .with_shutdown(shutdown.clone())
}

fn configured_ping(
    lightwalletd_config: &LightwalletdConfig,
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
