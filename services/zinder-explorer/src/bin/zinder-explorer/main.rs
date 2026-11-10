//! Zinder explorer-plane gRPC server entry point.

use std::{net::SocketAddr, path::PathBuf, process::ExitCode, sync::Arc, time::Duration};
use zinder_core::NetworkUpgradeActivations;
use zinder_core::wire::encode_zinder_native_chain_name;

use clap::Parser;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zinder_explorer::{
    ExplorerQueryGrpcAdapter, ExplorerServerInfoSettings, MaterializedViewStore,
    MaterializedViewStoreError, MaterializedViewStoreOptions, describe_request_metrics,
};
use zinder_runtime::{
    ComposedReadiness, OpsEndpointHandle, Readiness, ReadinessCause, ReadinessInput,
    ReadinessState, RuntimeService, StartupPhase, cancel_on_terminating_signal,
    host_cpu_meets_compiled_baseline, install_tracing_subscriber, spawn_ops_endpoint_for,
};
use zinder_source::{NodeTarget, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};

mod config;

use config::{ExplorerConfig, ExplorerConfigError, ExplorerConfigOverrides};

/// Cadence the background task uses to advance the secondary's view to the
/// primary's latest durable state.
const MATERIALIZED_VIEW_CATCHUP_INTERVAL: Duration = Duration::from_secs(1);

/// Default cadence for the upstream-observation probe.
///
/// Used when the resolved [`NodeTarget`] does not pin a `[node.health]`
/// `poll_interval`. Mirrors the source plane's default so an
/// explorer-side operator sees the same cadence as the ingest-side
/// probe.
const DEFAULT_UPSTREAM_OBSERVATION_POLL_INTERVAL: Duration =
    Duration::from_millis(zinder_source::DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS);

#[derive(Parser)]
#[command(name = "zinder-explorer")]
#[command(about = "Zinder explorer-plane gRPC server")]
#[command(version)]
struct Cli {
    /// TOML configuration file loaded before environment variables and CLI overrides.
    #[arg(long = "config", global = true)]
    config_path: Option<PathBuf>,
    /// Print the resolved command configuration without opening storage or binding.
    #[arg(long = "print-config", global = true)]
    print_config: bool,
    /// Network name, such as zcash-regtest.
    #[arg(long)]
    network: Option<String>,
    /// Canonical storage root containing the explorer materialized-view store.
    #[arg(long = "storage-path")]
    storage_path: Option<PathBuf>,
    /// Process-unique metadata root for explorer `RocksDB` secondaries.
    #[arg(long = "secondary-path")]
    secondary_path: Option<PathBuf>,
    /// `ExplorerQuery` gRPC listen address, such as 127.0.0.1:9068.
    #[arg(long = "listen-addr")]
    listen_addr: Option<SocketAddr>,
    /// Path to a file containing the shared-secret bearer token enforced by
    /// the `ExplorerQuery` endpoint.
    #[arg(long = "bearer-token-path")]
    bearer_token_path: Option<PathBuf>,
    /// Operational HTTP endpoint listen address for /healthz, /readyz, /metrics.
    #[arg(long = "ops-listen-addr")]
    ops_listen_addr: Option<SocketAddr>,
    /// `WalletQuery` gRPC endpoint that backs the federated read paths.
    /// Empty/unset disables every capability that needs upstream reads.
    #[arg(long = "wallet-query-endpoint")]
    wallet_query_endpoint: Option<String>,
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

#[allow(
    clippy::too_many_lines,
    reason = "startup keeps configuration, secondary admission, and server wiring in one path"
)]
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
    let readiness = Readiness::default();
    readiness.set(ReadinessState::starting());
    let start_api_phase = StartupPhase::StartApi.start();

    let materialized_view_store = match open_materialized_view_store(&explorer_config) {
        Ok(materialized_view_store) => materialized_view_store,
        Err(error) => {
            start_api_phase.fail(&error);
            return Err(error);
        }
    };
    report_materialized_view_workload(&readiness, materialized_view_store.as_ref());
    let required_inputs: &[ReadinessInput] = if materialized_view_store.is_some() {
        &[ReadinessInput::MaterializedViewCatchup]
    } else {
        &[]
    };
    let composed_readiness = ComposedReadiness::new(readiness.clone(), required_inputs);

    let cancel = CancellationToken::new();
    let _signal_handle = cancel_on_terminating_signal(cancel.clone());

    let materialized_view_catchup_handle =
        materialized_view_store
            .clone()
            .map(|materialized_view_store| {
                spawn_materialized_view_catchup_task(
                    move || materialized_view_store.try_catch_up(),
                    composed_readiness.clone(),
                    MATERIALIZED_VIEW_CATCHUP_INTERVAL,
                    cancel.clone(),
                )
            });
    let grpc_adapter = build_grpc_adapter(&explorer_config, materialized_view_store).await;
    let upstream_observation_handle =
        spawn_upstream_observation_probe(&explorer_config, &grpc_adapter, cancel.clone())?;
    let advertised_capabilities = grpc_adapter.advertised_capabilities();

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

    let server_result = tonic::transport::Server::builder()
        .add_service(grpc_adapter.into_server())
        .serve_with_shutdown(
            explorer_config.listen_addr,
            drain_readiness_on_cancel(cancel.clone(), composed_readiness),
        )
        .await;

    tracing::info!(
        target: "zinder::explorer",
        event = "explorer_stopped",
        "explorer query gRPC server stopped"
    );

    let background_shutdown_result = shutdown_background_tasks(
        ops_handle,
        upstream_observation_handle,
        materialized_view_catchup_handle,
    )
    .await;

    match server_result {
        Err(error) => {
            if let Err(ops_error) = background_shutdown_result {
                tracing::warn!(
                    target: "zinder::explorer",
                    event = "ops_endpoint_shutdown_failed",
                    error = %ops_error,
                    "operational endpoint shutdown also failed"
                );
            }
            Err(ExplorerConfigError::Transport(error))
        }
        Ok(()) => background_shutdown_result.map_err(ExplorerConfigError::from),
    }
}

async fn drain_readiness_on_cancel(cancel: CancellationToken, readiness: ComposedReadiness) {
    cancel.cancelled().await;
    readiness.publish_shutting_down();
}

fn report_materialized_view_workload(
    readiness: &Readiness,
    materialized_view_store: Option<&MaterializedViewStore>,
) {
    let Some(materialized_view_preset) =
        materialized_view_store.map(MaterializedViewStore::effective_materialized_view_preset)
    else {
        return;
    };
    readiness.set_materialized_view_workload(
        materialized_view_preset.as_str(),
        materialized_view_preset
            .consumer_schemas()
            .iter()
            .map(|schema| schema.name.as_str().to_owned())
            .collect(),
    );
}

async fn shutdown_background_tasks(
    ops_handle: Option<OpsEndpointHandle>,
    upstream_observation_handle: Option<JoinHandle<()>>,
    materialized_view_catchup_handle: Option<JoinHandle<()>>,
) -> Result<(), zinder_runtime::OpsServerError> {
    let ops_shutdown_result = match ops_handle {
        Some(handle) => handle.shutdown().await,
        None => Ok(()),
    };
    if let Some(handle) = upstream_observation_handle {
        let _ = handle.await;
    }
    if let Some(handle) = materialized_view_catchup_handle {
        let _ = handle.await;
    }
    ops_shutdown_result
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
        },
    )?;
    Ok(source.with_health_config(node.health.clone()))
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
    let materialized_view_preset =
        match MaterializedViewStore::detect_materialized_view_preset_at_path(
            &materialized_view_path,
        ) {
            Ok(Some(materialized_view_preset)) => materialized_view_preset,
            Ok(None) => {
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
        };
    match MaterializedViewStore::open_secondary_with_materialized_view_preset(
        &materialized_view_path,
        &secondary_path,
        materialized_view_preset,
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
    CatchUp: Fn() -> Result<(), MaterializedViewStoreError> + Send + 'static,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let cause = match catch_up() {
                        Ok(()) => ReadinessCause::Ready,
                        Err(error) => {
                            tracing::warn!(
                                target: "zinder::explorer",
                                event = "materialized_view_secondary_catchup_failed",
                                error = %error,
                                "materialized-view store secondary catchup failed"
                            );
                            materialized_view_catchup_failure_cause(&error)
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
    ) {
        ReadinessCause::SchemaMismatch
    } else {
        ReadinessCause::StorageUnavailable
    }
}

/// Fetches the node-advertised network-upgrade activation table.
///
/// Feeds the `NetworkUpgradeStatus` handler. Returns `None` when no `[node]`
/// section is configured or the upstream fetch fails; the adapter then serves
/// an empty table.
async fn fetch_network_upgrade_activations(
    explorer_config: &ExplorerConfig,
) -> Option<Arc<NetworkUpgradeActivations>> {
    let node = explorer_config.node.as_ref()?;
    let source = match build_zebra_json_rpc_source(node) {
        Ok(source) => source,
        Err(error) => {
            tracing::warn!(
                target: "zinder::explorer",
                event = "network_upgrade_activations_source_build_failed",
                error = %error,
                "could not build node source for network-upgrade activations; \
                 NetworkUpgradeStatus serves an empty table"
            );
            return None;
        }
    };
    match source.fetch_network_upgrade_activations().await {
        Ok(activations) => Some(Arc::new(activations)),
        Err(error) => {
            tracing::warn!(
                target: "zinder::explorer",
                event = "network_upgrade_activations_fetch_failed",
                error = %error,
                "could not fetch network-upgrade activations from the node; \
                 NetworkUpgradeStatus serves an empty table"
            );
            None
        }
    }
}

async fn build_grpc_adapter(
    explorer_config: &ExplorerConfig,
    materialized_view_store: Option<MaterializedViewStore>,
) -> ExplorerQueryGrpcAdapter {
    let server_info = ExplorerServerInfoSettings {
        network: explorer_config.network,
    };
    let has_materialized_view_store = materialized_view_store.is_some();
    let mut grpc_adapter = ExplorerQueryGrpcAdapter::new(server_info)
        .with_prevout_resolution_online(has_materialized_view_store);
    if let Some(materialized_view_store) = materialized_view_store {
        grpc_adapter = grpc_adapter.with_materialized_view_store(materialized_view_store);
    }
    if let Some(activations) = fetch_network_upgrade_activations(explorer_config).await {
        grpc_adapter = grpc_adapter.with_network_upgrade_activations(activations);
    }
    if let Some(endpoint) = explorer_config.wallet_query_endpoint.clone() {
        grpc_adapter = grpc_adapter.with_wallet_query_endpoint(endpoint);
    }
    if let Some(token) = explorer_config.bearer_token.clone() {
        grpc_adapter = grpc_adapter.with_bearer_token(token);
    }
    grpc_adapter
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
    use zinder_explorer::MaterializedViewStoreError;
    use zinder_runtime::{
        ComposedReadiness, Readiness, ReadinessCause, ReadinessInput, ReadinessState,
    };

    use super::{drain_readiness_on_cancel, spawn_materialized_view_catchup_task};

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

    #[tokio::test]
    async fn cancellation_drains_readiness_before_server_shutdown() {
        let readiness = Readiness::default();
        let composed = ComposedReadiness::new(readiness.clone(), &[]);
        composed.publish_admission(ReadinessState::ready(None));
        let cancel = CancellationToken::new();
        cancel.cancel();

        drain_readiness_on_cancel(cancel, composed).await;

        assert_eq!(readiness.report().cause, ReadinessCause::ShuttingDown);
        assert!(!readiness.report().is_ready);
    }
}
