//! Reusable runtime entrypoints and CLI parsing for `zinder-explorer`.

pub mod config;

use std::{
    ffi::OsString,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
    time::Duration,
};

use clap::{Parser, Subcommand};
use parking_lot::RwLock;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use zinder_core::wire::encode_zinder_native_chain_name;
use zinder_proto::v1::explorer::explorer_query_server::SERVICE_NAME;
use zinder_runtime::{
    ComposedReadiness, OpsEndpointHandle, OpsHealthcheckArgs, OpsServerError, Readiness,
    ReadinessCause, ReadinessInput, ReadinessState, RuntimeService, RuntimeSupervisionError,
    StartupPhase, SupervisedRuntime, SupervisedTasks, TracingSubscriberError, guard_serving_bind,
    host_cpu_meets_compiled_baseline, host_cpu_meets_pasta_backend_baseline,
    install_metrics_recorder_for_service, install_tracing_subscriber,
    section_defaults::default_ops_listen_addr, spawn_ops_endpoint_for,
};
use zinder_source::{NodeTarget, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_store::{
    CanonicalStoreError, CanonicalStoreRequirements, RocksDbCanonicalSecondary,
    RocksDbCanonicalStore,
};

use crate::{
    ExplorerQueryGrpcAdapter, ExplorerServerInfoSettings, MaterializedViewStore,
    MaterializedViewStoreError, MaterializedViewStoreOptions, describe_request_metrics,
};

pub use config::{
    ExplorerConfig, ExplorerConfigError, ExplorerConfigOverrides, explorer_config_toml,
    load_explorer_config, load_explorer_config_with_env,
};

type SharedCanonicalSecondary = Arc<RwLock<RocksDbCanonicalSecondary>>;

/// Default cadence for the upstream-observation probe.
///
/// Used when the resolved [`NodeTarget`] does not pin a `[node.health]`
/// `poll_interval`. Mirrors the source plane's default so an
/// explorer-side operator sees the same cadence as the ingest-side
/// probe.
const DEFAULT_UPSTREAM_OBSERVATION_POLL_INTERVAL: Duration =
    Duration::from_millis(zinder_source::DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS);

/// Command-line interface for the `zinder-explorer` gRPC server.
#[derive(Debug, Parser)]
#[command(name = "zinder-explorer")]
#[command(about = "Zinder explorer-plane gRPC server")]
#[command(version)]
pub struct Cli {
    /// TOML configuration file loaded before environment variables and CLI overrides.
    #[arg(long = "config", global = true)]
    pub config_path: Option<PathBuf>,
    /// Print the resolved command configuration without opening storage or binding.
    #[arg(long = "print-config", global = true)]
    pub print_config: bool,
    /// Network name, such as zcash-regtest.
    #[arg(long)]
    pub network: Option<String>,
    /// Canonical storage root containing the explorer materialized-view store.
    #[arg(long = "storage-path")]
    pub storage_path: Option<PathBuf>,
    /// Process-unique metadata root for explorer `RocksDB` secondaries.
    #[arg(long = "secondary-path")]
    pub secondary_path: Option<PathBuf>,
    /// `ExplorerQuery` gRPC listen address, such as 127.0.0.1:9068.
    #[arg(long = "listen-addr")]
    pub listen_addr: Option<SocketAddr>,
    /// Path to a file containing the shared-secret bearer token enforced by
    /// the `ExplorerQuery` endpoint.
    #[arg(long = "bearer-token-path")]
    pub bearer_token_path: Option<PathBuf>,
    /// Operational HTTP endpoint listen address for /healthz, /readyz, /metrics.
    #[arg(long = "ops-listen-addr")]
    pub ops_listen_addr: Option<SocketAddr>,
    /// `WalletQuery` gRPC endpoint that backs the federated read paths.
    /// Empty/unset disables every capability that needs upstream reads.
    #[arg(long = "wallet-query-endpoint")]
    pub wallet_query_endpoint: Option<String>,
    /// Optional operational subcommand.
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Subcommands supported by `zinder-explorer`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Probe the operational endpoint once and exit 0 only when it answers
    /// HTTP 200. Reads only `[ops] listen_addr` from the `--config` file.
    Healthcheck(OpsHealthcheckArgs),
}

/// Runs the standalone `zinder-explorer` CLI from process arguments.
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

/// Supervises the explorer server inside a parent runtime such as `zinder serve`.
///
/// The caller provides a bound gRPC listener and owns the operational HTTP
/// endpoint, metrics recorder, CPU checks, and terminating-signal handling. `readiness` receives explorer readiness
/// updates and `cancel` triggers bounded shutdown.
pub async fn serve_supervised(
    mut explorer_config: ExplorerConfig,
    readiness: Readiness,
    cancel: CancellationToken,
    listener: TcpListener,
) -> Result<(), ExplorerConfigError> {
    explorer_config.listen_addr = listener
        .local_addr()
        .map_err(|source| RuntimeSupervisionError::GrpcServer { source })?;
    guard_serving_bind(
        "explorer.listen_addr",
        explorer_config.listen_addr,
        explorer_config.allow_public_bind,
    )?;
    explorer_config.ops_listen_addr = None;
    let runtime = SupervisedRuntime::new(RuntimeService::Explorer);
    let shutdown = runtime.shutdown_request();
    let cancel_bridge = tokio::spawn(async move {
        cancel.cancelled().await;
        shutdown.request();
    });
    let outcome = serve_explorer(explorer_config, runtime, readiness, Some(listener)).await;
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
        RuntimeService::Explorer,
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
    let render_result = config::load_explorer_config(config_path, cli.into())
        .and_then(|explorer_config| config::explorer_config_toml(&explorer_config));

    match render_result {
        Ok(rendered_toml) => {
            println!("{rendered_toml}");
            ExitCode::SUCCESS
        }
        Err(error) => emit_runtime_error(&error),
    }
}

async fn run_runtime(cli: Cli) -> ExitCode {
    match run_explorer(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => emit_runtime_error(&error),
    }
}

async fn run_explorer(cli: Cli) -> Result<(), ExplorerConfigError> {
    let load_config_phase = StartupPhase::LoadConfig.start();
    let config_path = cli.config_path.clone();
    let explorer_config = match config::load_explorer_config(config_path, cli.into()) {
        Ok(cfg) => {
            load_config_phase.complete();
            cfg
        }
        Err(error) => {
            load_config_phase.fail(&error);
            return Err(error);
        }
    };
    let runtime = SupervisedRuntime::new(RuntimeService::Explorer);
    let _signal_handle = runtime.request_shutdown_on_terminating_signal();
    serve_explorer(explorer_config, runtime, Readiness::default(), None).await
}

fn open_explorer_stores(
    explorer_config: &ExplorerConfig,
) -> Result<
    (
        Option<SharedCanonicalSecondary>,
        Option<MaterializedViewStore>,
    ),
    ExplorerConfigError,
> {
    let canonical_secondary = open_canonical_secondary(explorer_config)?;
    let materialized_view_store = open_materialized_view_store(explorer_config)?;
    Ok((canonical_secondary, materialized_view_store))
}

fn build_materialized_view_catchup_task(
    explorer_config: &ExplorerConfig,
    materialized_view_store: Option<MaterializedViewStore>,
    canonical_secondary: Option<SharedCanonicalSecondary>,
    composed_readiness: &ComposedReadiness,
    stopping: CancellationToken,
) -> Option<JoinHandle<()>> {
    match (materialized_view_store, canonical_secondary) {
        (None, None) => None,
        (mv_store, canonical_store) => {
            let readiness = composed_readiness.runtime_readiness();
            Some(spawn_materialized_view_catchup_task(
                move || {
                    let outcome = catch_up_secondaries(mv_store.as_ref(), canonical_store.as_ref());
                    publish_served_epoch(&readiness, canonical_store.as_ref());
                    outcome
                },
                composed_readiness.clone(),
                explorer_config.storage.secondary_catchup_interval,
                stopping,
            ))
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the linear startup sequence keeps operator-visible failure ordering auditable"
)]
async fn serve_explorer(
    explorer_config: ExplorerConfig,
    runtime: SupervisedRuntime,
    readiness: Readiness,
    listener: Option<TcpListener>,
) -> Result<(), ExplorerConfigError> {
    let start_api_phase = StartupPhase::StartApi.start();

    if explorer_config.ops_listen_addr.is_some()
        && let Err(error) = install_metrics_recorder_for_service(
            RuntimeService::Explorer,
            env!("CARGO_PKG_VERSION"),
            encode_zinder_native_chain_name(explorer_config.network),
        )
        .map_err(OpsServerError::from)
    {
        start_api_phase.fail(&error);
        return Err(error.into());
    }

    let (canonical_secondary, materialized_view_store) =
        match open_explorer_stores(&explorer_config) {
            Ok(stores) => stores,
            Err(error) => {
                start_api_phase.fail(&error);
                return Err(error);
            }
        };
    report_materialized_view_identities(&readiness, materialized_view_store.as_ref());
    publish_served_epoch(&readiness, canonical_secondary.as_ref());
    let required_inputs: &[ReadinessInput] =
        if materialized_view_store.is_some() || canonical_secondary.is_some() {
            &[ReadinessInput::MaterializedViewCatchup]
        } else {
            &[]
        };
    let composed_readiness = ComposedReadiness::new(readiness.clone(), required_inputs);

    let grpc_adapter = build_grpc_adapter(
        &explorer_config,
        canonical_secondary.clone(),
        materialized_view_store.clone(),
    );
    let advertised_capabilities = grpc_adapter.advertised_capabilities();
    let router = runtime.public_grpc_router_with_transport(
        (
            grpc_adapter.clone().into_server(),
            zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET,
        ),
        explorer_config.client_address_source,
        &readiness,
        |server| server.with_browser_transport(&explorer_config.browser_origins, SERVICE_NAME),
    )?;
    let ops_handle = spawn_ops_endpoint_for(
        RuntimeService::Explorer,
        explorer_config.ops_listen_addr,
        env!("CARGO_PKG_VERSION"),
        encode_zinder_native_chain_name(explorer_config.network),
        readiness.clone(),
        Arc::from(advertised_capabilities),
    )
    .await?;
    describe_request_metrics();
    let upstream_observation =
        spawn_upstream_observation_probe(&explorer_config, &grpc_adapter, runtime.stopping())?;
    let materialized_view_catchup = build_materialized_view_catchup_task(
        &explorer_config,
        materialized_view_store,
        canonical_secondary,
        &composed_readiness,
        runtime.stopping(),
    );
    let tasks = supervised_tasks(ops_handle, upstream_observation, materialized_view_catchup);

    start_api_phase.complete();
    StartupPhase::Ready.start().complete();
    composed_readiness.publish_admission(ReadinessState::ready(None));

    tracing::info!(
        target: "zinder::explorer",
        event = "explorer_started",
        network = encode_zinder_native_chain_name(explorer_config.network),
        listen_addr = %explorer_config.listen_addr,
        storage_path = %explorer_config.storage.path.display(),
        "explorer query gRPC server started"
    );

    let shutdown = runtime.grpc_server_shutdown();
    let server = async move {
        if let Some(listener) = listener {
            router
                .serve_with_incoming_shutdown(listener, shutdown)
                .await
        } else {
            router
                .serve_with_shutdown(explorer_config.listen_addr, shutdown)
                .await
        }
    };
    runtime
        .supervise(server, tasks, || composed_readiness.publish_shutting_down())
        .await?;

    tracing::info!(
        target: "zinder::explorer",
        event = "explorer_stopped",
        "explorer query gRPC server stopped"
    );
    Ok(())
}

fn publish_served_epoch(
    readiness: &Readiness,
    canonical_secondary: Option<&SharedCanonicalSecondary>,
) {
    if let Some(epoch) = canonical_secondary.and_then(|store| store.read().chain_epoch().ok()) {
        readiness.set_served_chain_epoch(&epoch);
    }
}

fn report_materialized_view_identities(
    readiness: &Readiness,
    materialized_view_store: Option<&MaterializedViewStore>,
) {
    let Some(materialized_view_store) = materialized_view_store else {
        return;
    };
    readiness.set_materialized_view_identities(
        materialized_view_store
            .consumer_identities()
            .map(|identity| identity.as_str().to_owned())
            .collect(),
    );
}

fn supervised_tasks(
    operations: Option<OpsEndpointHandle>,
    upstream_observation: Option<JoinHandle<()>>,
    materialized_view_catchup: Option<JoinHandle<()>>,
) -> SupervisedTasks {
    let mut tasks = SupervisedTasks::new().with_operations_endpoint(operations);
    if let Some(handle) = upstream_observation {
        tasks = tasks.with("upstream-observation probe", handle);
    }
    if let Some(handle) = materialized_view_catchup {
        tasks = tasks.with("materialized-view catch-up", handle);
    }
    tasks
}

fn spawn_upstream_observation_probe(
    explorer_config: &ExplorerConfig,
    grpc_adapter: &ExplorerQueryGrpcAdapter,
    cancel: CancellationToken,
) -> Result<Option<JoinHandle<()>>, ExplorerConfigError> {
    let Some(node) = explorer_config.node.as_ref() else {
        tracing::info!(
            target: "zinder::explorer",
            event = "upstream_observation_probe_skipped",
            reason = "no [node] section configured; ExplorerFreshness.chain_view.upstream_tip stays unset",
        );
        return Ok(None);
    };
    let source = build_zebra_json_rpc_source(node)?;
    let poll_interval = node
        .health
        .as_ref()
        .map_or(DEFAULT_UPSTREAM_OBSERVATION_POLL_INTERVAL, |health| {
            health.poll_interval
        });
    tracing::info!(
        target: "zinder::explorer",
        event = "upstream_observation_probe_started",
        json_rpc_addr = node.json_rpc_addr.as_str(),
        poll_interval_ms = u64::try_from(poll_interval.as_millis()).unwrap_or(u64::MAX),
        "upstream observation probe started",
    );
    Ok(Some(grpc_adapter.spawn_upstream_observation_probe(
        Arc::new(source),
        poll_interval,
        cancel,
    )))
}

fn build_zebra_json_rpc_source(
    node: &NodeTarget,
) -> Result<ZebraJsonRpcSource, ExplorerConfigError> {
    let source = ZebraJsonRpcSource::with_options(
        node.network,
        &node.json_rpc_addr,
        node.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: node.request_timeout,
            max_response_bytes: node.max_response_bytes,
            broadcast_timeout: node.broadcast_timeout,
            max_concurrent_requests: node.max_concurrent_requests,
        },
    )?;
    Ok(source.with_health_config(node.health.clone()))
}

fn open_canonical_secondary(
    explorer_config: &ExplorerConfig,
) -> Result<Option<SharedCanonicalSecondary>, ExplorerConfigError> {
    let primary_path = &explorer_config.storage.path;
    let current_exists = primary_path
        .join("CURRENT")
        .try_exists()
        .map_err(|source| {
            canonical_secondary_error(
                "probe canonical primary",
                CanonicalStoreError::PathUnavailable {
                    path: primary_path.clone(),
                    source,
                },
            )
        })?;
    if !current_exists {
        tracing::info!(
            target: "zinder::explorer",
            event = "canonical_secondary_unavailable",
            "canonical primary store is absent; canonical-backed explorer capabilities disabled"
        );
        return Ok(None);
    }

    let open_storage_phase = StartupPhase::OpenStorage.start();
    let summary = match RocksDbCanonicalStore::read_ready_summary(primary_path) {
        Ok(Some(summary)) => summary,
        Ok(None) => {
            tracing::info!(
                target: "zinder::explorer",
                event = "canonical_secondary_unavailable",
                "canonical primary store is not READY; canonical-backed explorer capabilities disabled"
            );
            open_storage_phase.complete();
            return Ok(None);
        }
        Err(source) => {
            let wrapped = canonical_secondary_error("read canonical store ready summary", source);
            open_storage_phase.fail(&wrapped);
            return Err(wrapped);
        }
    };

    let secondary_path = explorer_config.storage.secondary_path.join("canonical");
    match RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        primary_path,
        &secondary_path,
        explorer_config.network,
        CanonicalStoreRequirements::default(),
        summary.reorg_policy(),
        explorer_config.storage.canonical_rocksdb_budget,
    ) {
        Ok(secondary) => {
            tracing::info!(
                target: "zinder::explorer",
                event = "canonical_secondary_opened",
                storage_path = %primary_path.display(),
                secondary_path = %secondary_path.display(),
                visible_tip_height = summary.visible_tip().height.value(),
                "canonical secondary store opened"
            );
            open_storage_phase.complete();
            Ok(Some(Arc::new(RwLock::new(secondary))))
        }
        Err(source) => {
            let wrapped = canonical_secondary_error("open canonical secondary", source);
            open_storage_phase.fail(&wrapped);
            Err(wrapped)
        }
    }
}

fn canonical_secondary_error(
    operation: &'static str,
    source: CanonicalStoreError,
) -> ExplorerConfigError {
    ExplorerConfigError::Store(MaterializedViewStoreError::CanonicalSecondary { operation, source })
}

fn catch_up_secondaries(
    materialized_view_store: Option<&MaterializedViewStore>,
    canonical_secondary: Option<&SharedCanonicalSecondary>,
) -> Result<(), MaterializedViewStoreError> {
    let mv_outcome = materialized_view_store.map_or(Ok(()), MaterializedViewStore::try_catch_up);
    let canonical_outcome = canonical_secondary.map_or(Ok(()), |store| {
        store.write().try_catch_up().map(|_| ()).map_err(|source| {
            MaterializedViewStoreError::CanonicalSecondary {
                operation: "catch up canonical secondary",
                source,
            }
        })
    });
    mv_outcome.and(canonical_outcome)
}

fn open_materialized_view_store(
    explorer_config: &ExplorerConfig,
) -> Result<Option<MaterializedViewStore>, ExplorerConfigError> {
    let materialized_view_path =
        MaterializedViewStore::path_for_canonical(&explorer_config.storage.path);
    let secondary_path = explorer_config
        .storage
        .secondary_path
        .join("materialized-views");
    let open_storage_phase = StartupPhase::OpenStorage.start();
    match MaterializedViewStore::exists_at_path(&materialized_view_path) {
        Ok(true) => {}
        Ok(false) => {
            tracing::info!(
                target: "zinder::explorer",
                event = "materialized_view_store_unavailable",
                "materialized-view store unavailable; materialized-view-backed explorer capabilities disabled"
            );
            open_storage_phase.complete();
            return Ok(None);
        }
        Err(error @ MaterializedViewStoreError::Open { .. }) => {
            tracing::info!(
                target: "zinder::explorer",
                event = "materialized_view_store_unavailable",
                error = %error,
                "materialized-view store unavailable; materialized-view-backed explorer capabilities disabled"
            );
            open_storage_phase.complete();
            return Ok(None);
        }
        Err(error) => {
            let wrapped = ExplorerConfigError::Store(error);
            open_storage_phase.fail(&wrapped);
            return Err(wrapped);
        }
    }
    match MaterializedViewStore::open_explorer_views_secondary(
        &materialized_view_path,
        &secondary_path,
        MaterializedViewStoreOptions {
            sync_writes: false,
            rocksdb_resource_budget: explorer_config.storage.materialized_view_rocksdb_budget,
            ..MaterializedViewStoreOptions::default()
        },
    ) {
        Ok(handle) => {
            open_storage_phase.complete();
            Ok(Some(handle))
        }
        Err(error @ MaterializedViewStoreError::Open { .. }) => {
            tracing::info!(
                target: "zinder::explorer",
                event = "materialized_view_store_unavailable",
                error = %error,
                "materialized-view store unavailable; materialized-view-backed explorer capabilities disabled"
            );
            open_storage_phase.complete();
            Ok(None)
        }
        Err(error) => {
            let wrapped = ExplorerConfigError::Store(error);
            open_storage_phase.fail(&wrapped);
            Err(wrapped)
        }
    }
}

fn spawn_materialized_view_catchup_task<CatchUp>(
    catch_up: CatchUp,
    readiness: ComposedReadiness,
    interval: Duration,
    cancel: CancellationToken,
) -> JoinHandle<()>
where
    CatchUp: Fn() -> Result<(), MaterializedViewStoreError> + Send + Sync + 'static,
{
    let catch_up = Arc::new(catch_up);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let catch_up_fn = Arc::clone(&catch_up);
                    let cause = match tokio::task::spawn_blocking(move || catch_up_fn()).await {
                        Ok(Ok(())) => ReadinessCause::Ready,
                        Ok(Err(error)) => {
                            tracing::warn!(
                                target: "zinder::explorer",
                                event = "materialized_view_secondary_catchup_failed",
                                error = %error,
                                "materialized-view store secondary catchup failed"
                            );
                            materialized_view_catchup_failure_cause(&error)
                        }
                        Err(join_error) => {
                            tracing::warn!(
                                target: "zinder::explorer",
                                event = "materialized_view_secondary_catchup_panicked",
                                error = %join_error,
                                "materialized-view store secondary catchup task failed"
                            );
                            ReadinessCause::StorageUnavailable
                        }
                    };
                    readiness.publish_input(ReadinessInput::MaterializedViewCatchup, cause);
                }
                () = cancel.cancelled() => break,
            }
        }
    })
}

const fn materialized_view_catchup_failure_cause(
    error: &MaterializedViewStoreError,
) -> ReadinessCause {
    if matches!(
        error,
        MaterializedViewStoreError::SchemaMismatch { .. }
            | MaterializedViewStoreError::ConsumerSchemaMismatch { .. }
            | MaterializedViewStoreError::ConsumerNotDeclared { .. }
            | MaterializedViewStoreError::ColumnFamilyIdentityMismatch { .. }
            | MaterializedViewStoreError::CanonicalSecondary {
                source: CanonicalStoreError::AdmissionRefused { .. },
                ..
            }
    ) {
        ReadinessCause::SchemaMismatch
    } else {
        ReadinessCause::StorageUnavailable
    }
}

fn build_grpc_adapter(
    explorer_config: &ExplorerConfig,
    canonical_secondary: Option<SharedCanonicalSecondary>,
    materialized_view_store: Option<MaterializedViewStore>,
) -> ExplorerQueryGrpcAdapter {
    let server_info = ExplorerServerInfoSettings {
        network: explorer_config.network,
    };
    let has_materialized_view_store = materialized_view_store.is_some();
    let mut grpc_adapter = ExplorerQueryGrpcAdapter::new(server_info)
        .with_prevout_resolution_online(has_materialized_view_store);
    if let Some(canonical_secondary) = canonical_secondary {
        grpc_adapter = grpc_adapter.with_canonical_store(canonical_secondary);
    }
    if let Some(materialized_view_store) = materialized_view_store {
        grpc_adapter = grpc_adapter.with_materialized_view_store(materialized_view_store);
    }
    if let Some(endpoint) = explorer_config.wallet_query_endpoint.clone() {
        grpc_adapter = grpc_adapter.with_wallet_query_endpoint(endpoint);
    }
    if let Some(token) = explorer_config.bearer_token.clone() {
        grpc_adapter = grpc_adapter.with_bearer_token(token);
    }
    grpc_adapter
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
        default_ops_listen_addr(RuntimeService::Explorer),
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

fn emit_runtime_error(error: &ExplorerConfigError) -> ExitCode {
    tracing::error!(
        target: "zinder::explorer",
        event = "explorer_run_failed",
        error = %error,
        "explorer run failed"
    );
    ExitCode::FAILURE
}

impl From<Cli> for ExplorerConfigOverrides {
    fn from(cli: Cli) -> Self {
        Self {
            network: cli.network,
            storage_path: cli.storage_path,
            secondary_path: cli.secondary_path,
            listen_addr: cli.listen_addr,
            ops_listen_addr: cli.ops_listen_addr,
            bearer_token_path: cli.bearer_token_path,
            wallet_query_endpoint: cli.wallet_query_endpoint,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use tokio_util::sync::CancellationToken;
    use zinder_runtime::{
        ComposedReadiness, Readiness, ReadinessCause, ReadinessInput, ReadinessState,
    };

    use super::spawn_materialized_view_catchup_task;
    use crate::MaterializedViewStoreError;

    async fn wait_for_cause(readiness: &Readiness, expected: &ReadinessCause) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while readiness.report().cause != *expected && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn persistent_catchup_failure_lowers_readiness_with_typed_cause()
    -> Result<(), tokio::task::JoinError> {
        let readiness = Readiness::default();
        let composed = ComposedReadiness::new(
            readiness.clone(),
            &[ReadinessInput::MaterializedViewCatchup],
        );
        composed.publish_admission(ReadinessState::ready(None));
        let schema_mismatch = Arc::new(AtomicBool::new(true));
        let cancel = CancellationToken::new();
        let handle = spawn_materialized_view_catchup_task(
            {
                let schema_mismatch = Arc::clone(&schema_mismatch);
                move || {
                    if schema_mismatch.load(Ordering::SeqCst) {
                        Err(MaterializedViewStoreError::SchemaMismatch {
                            persisted: 1,
                            running: 2,
                        })
                    } else {
                        Err(MaterializedViewStoreError::ConsumerManifest {
                            operation: "read",
                            reason: "unreadable manifest".to_owned(),
                        })
                    }
                }
            },
            composed.clone(),
            Duration::from_millis(1),
            cancel.clone(),
        );

        wait_for_cause(&readiness, &ReadinessCause::SchemaMismatch).await;
        let schema_cause = readiness.report().cause;
        schema_mismatch.store(false, Ordering::SeqCst);
        wait_for_cause(&readiness, &ReadinessCause::StorageUnavailable).await;
        let storage_cause = readiness.report().cause;
        cancel.cancel();
        handle.await?;

        assert_eq!(schema_cause, ReadinessCause::SchemaMismatch);
        assert_eq!(storage_cause, ReadinessCause::StorageUnavailable);
        assert!(!readiness.report().is_ready);
        Ok(())
    }

    #[tokio::test]
    async fn successful_catchup_restores_readiness() -> Result<(), tokio::task::JoinError> {
        let readiness = Readiness::default();
        let composed = ComposedReadiness::new(
            readiness.clone(),
            &[ReadinessInput::MaterializedViewCatchup],
        );
        composed.publish_admission(ReadinessState::ready(None));
        composed.publish_input(
            ReadinessInput::MaterializedViewCatchup,
            ReadinessCause::StorageUnavailable,
        );
        let cancel = CancellationToken::new();
        let handle = spawn_materialized_view_catchup_task(
            || Ok(()),
            composed,
            Duration::from_millis(1),
            cancel.clone(),
        );

        wait_for_cause(&readiness, &ReadinessCause::Ready).await;
        cancel.cancel();
        handle.await?;

        assert!(readiness.report().is_ready);
        Ok(())
    }

    #[test]
    fn catch_up_secondaries_advances_canonical_when_materialized_view_catchup_fails()
    -> Result<(), Box<dyn std::error::Error>> {
        use parking_lot::RwLock;
        use tempfile::tempdir;
        use zinder_core::{BlockHeight, Network};
        use zinder_materialized_views::MaterializedViewStore;
        use zinder_store::{
            CanonicalReorgPolicy, CanonicalStoreRequirements, RawBlobRetention,
            RocksDbCanonicalSecondary, RocksDbResourceBudget,
        };
        use zinder_testkit::{
            CanonicalBaseline, CanonicalWriterFixture, ChainFixture,
            sample_regtest_upgrade_activations,
        };

        use crate::MaterializedViewStoreOptions;

        let temp = tempdir()?;
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
        let activations = sample_regtest_upgrade_activations();
        let mut writer = CanonicalWriterFixture::publish(
            &CanonicalBaseline::new(&chain).through(BlockHeight::new(1)),
            &activations,
        )?;
        let canonical_secondary = Arc::new(RwLock::new(
            RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
                writer.canonical_primary_path(),
                temp.path().join("canonical-secondary"),
                Network::ZcashRegtest,
                CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
                CanonicalReorgPolicy::new(100)?,
                RocksDbResourceBudget::for_local_tests(),
            )?,
        ));
        let mv_primary_path = temp.path().join("mv-primary");
        let mv_primary = MaterializedViewStore::open_explorer_views(
            &mv_primary_path,
            MaterializedViewStoreOptions {
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                ..MaterializedViewStoreOptions::default()
            },
        )?;
        let mv_secondary = MaterializedViewStore::open_explorer_views_secondary(
            &mv_primary_path,
            temp.path().join("mv-secondary"),
            MaterializedViewStoreOptions {
                sync_writes: false,
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                ..MaterializedViewStoreOptions::default()
            },
        )?;

        drop(mv_primary);
        std::fs::remove_dir_all(&mv_primary_path)?;
        writer.append_live_block(&chain)?;

        let outcome = super::catch_up_secondaries(Some(&mv_secondary), Some(&canonical_secondary));
        assert!(outcome.is_err());
        assert_eq!(
            canonical_secondary.read().chain_epoch()?.visible_tip_height,
            BlockHeight::new(2)
        );
        Ok(())
    }
}

#[cfg(test)]
mod supervision_tests;
