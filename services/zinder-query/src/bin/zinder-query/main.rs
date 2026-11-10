//! Zinder native wallet-query gRPC server entry point.

use std::{future::Future, net::SocketAddr, path::PathBuf, process::ExitCode, sync::Arc};

use clap::Parser;
use tokio_util::sync::CancellationToken;
use zinder_core::wire::encode_zinder_native_chain_name;
use zinder_query::{
    AdmittedIngestControl, WalletEndpointMetadata, WalletQueryApi, WalletQueryGrpcAdapter,
    WalletServingPairConfig, WalletServingPairPublisher, WalletServingQuery,
    WalletServingReadiness, spawn_wallet_ingest_control_readiness_probe,
    spawn_wallet_node_readiness_probe,
};
use zinder_runtime::{
    OpsEndpointHandle, OpsServerError, Readiness, RuntimeService, StartupPhase,
    TrafficReadinessInterceptor, cancel_on_terminating_signal,
    install_metrics_recorder_for_service, install_tracing_subscriber, spawn_ops_endpoint_for,
};
use zinder_source::{
    DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions,
};

mod config;

use config::{QueryConfigError, QueryConfigOverrides};

#[derive(Parser)]
#[command(name = "zinder-query")]
#[command(about = "Zinder native WalletQuery gRPC server")]
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
    /// Canonical primary path replicated only through an immutable secondary.
    #[arg(long = "canonical-primary-path")]
    canonical_primary_path: Option<PathBuf>,
    /// Root containing this process's canonical secondary generations.
    #[arg(long = "canonical-secondary-root")]
    canonical_secondary_root: Option<PathBuf>,
    /// Canonical raw-blob retention expected from the writer.
    #[arg(
        long = "raw-blob-policy",
        value_parser = ["none", "transactions", "all"]
    )]
    raw_blob_policy: Option<String>,
    /// Wallet primary path replicated only through an immutable secondary.
    #[arg(long = "wallet-primary-path")]
    wallet_primary_path: Option<PathBuf>,
    /// Root containing this process's wallet secondary generations.
    #[arg(long = "wallet-secondary-root")]
    wallet_secondary_root: Option<PathBuf>,
    /// Private `zinder-ingest` control gRPC endpoint.
    #[arg(long = "ingest-control-addr")]
    ingest_control_addr: Option<String>,
    /// File containing the shared-secret bearer token used by `IngestControl`.
    #[arg(long = "ingest-control-token-path")]
    ingest_control_token_path: Option<PathBuf>,
    /// Native `WalletQuery` gRPC listen address.
    #[arg(long = "listen-addr")]
    listen_addr: Option<SocketAddr>,
    /// Exact canonical replacement-depth identity expected from the writer.
    #[arg(long = "reorg-window-blocks")]
    reorg_window_blocks: Option<u32>,
    /// Operational HTTP endpoint listen address for health and metrics.
    #[arg(long = "ops-listen-addr")]
    ops_listen_addr: Option<SocketAddr>,
    /// Zebra JSON-RPC address used to discover the canonical activation table.
    #[arg(long = "node-json-rpc-addr")]
    node_json_rpc_addr: Option<String>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    install_tracing_subscriber();
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

#[allow(
    clippy::too_many_lines,
    reason = "the linear startup sequence keeps operator-visible failure ordering auditable"
)]
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

    let readiness = Readiness::default();
    let serving_readiness =
        WalletServingReadiness::awaiting_node_and_ingest_control(readiness.clone());
    let admit_ingest_control_phase = StartupPhase::AdmitIngestControl.start();
    let ingest_control = match AdmittedIngestControl::connect(
        &query_config.ingest_control_addr,
        query_config.ingest_control_bearer_token.as_ref(),
        query_config.network,
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
    let connect_node_phase = StartupPhase::ConnectNode.start();
    let source = ZebraJsonRpcSource::with_options(
        query_config.node.network,
        query_config.node.json_rpc_addr.clone(),
        query_config.node.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: query_config.node.request_timeout,
            max_response_bytes: query_config.node.max_response_bytes,
            broadcast_timeout: query_config.node.broadcast_timeout,
        },
    )
    .map_err(|error| QueryConfigError::Source(Box::new(error)))?
    .with_health_config(query_config.node.health.clone());
    source
        .probe_capabilities()
        .await
        .map_err(|error| QueryConfigError::Source(Box::new(error)))?;
    let network_upgrade_activations = source
        .discover_network_upgrade_activations("zinder-query")
        .await
        .map_err(|error| QueryConfigError::Source(Box::new(error)))?;
    connect_node_phase.complete();

    let open_storage_phase = StartupPhase::OpenStorage.start();
    let (pair_publisher, pair_slot) =
        WalletServingPairPublisher::bootstrap_with_admitted_ingest_control(
            WalletServingPairConfig {
                canonical_primary_path: query_config.storage.path.clone(),
                canonical_secondary_root: query_config.storage.secondary_path.clone(),
                wallet_primary_path: query_config.wallet_primary_path.clone(),
                wallet_secondary_root: query_config.wallet_secondary_root.clone(),
                network: query_config.network,
                network_upgrade_activations: Arc::clone(&network_upgrade_activations),
                expected_raw_blob_retention: query_config.storage.expected_raw_blob_retention,
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
            },
            serving_readiness.clone(),
            &ingest_control,
        )
        .await?;
    let visible_height = pair_slot
        .capture()
        .canonical_fence()
        .visible_tip()
        .height
        .value();
    open_storage_phase.complete();

    let query = WalletServingQuery::from_admitted_native_sources(
        pair_slot,
        source.clone(),
        ingest_control.clone(),
        Arc::clone(&network_upgrade_activations),
    )?;
    let endpoint_metadata = WalletEndpointMetadata {
        network: encode_zinder_native_chain_name(query_config.network).to_owned(),
        service_version: env!("CARGO_PKG_VERSION").to_owned(),
        schema_version: u32::from(zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION.value()),
        reorg_window_blocks: query_config.canonical_reorg_policy.reorg_window_blocks(),
        materialized_view_preset: Some(zinder_materialized_views::MaterializedViewPreset::Wallet),
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
    let grpc_adapter = WalletQueryGrpcAdapter::new(query, endpoint_metadata)
        .with_traffic_readiness(readiness.clone());

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET)
        .build_v1()?;
    let cancel = CancellationToken::new();
    let _signal_handle = cancel_on_terminating_signal(cancel.clone());
    let node_readiness_handle = spawn_wallet_node_readiness_probe(
        source,
        &native_endpoint_capabilities,
        serving_readiness.clone(),
        query_config.node.health.as_ref().map_or_else(
            || std::time::Duration::from_millis(DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS),
            |health| health.poll_interval,
        ),
        cancel.clone(),
    )?;
    // Health and serving-pair refresh observe the same ingest control plane, so
    // they intentionally share one configured freshness cadence.
    let ingest_control_health_poll_interval = query_config.storage.secondary_catchup_interval;
    let ingest_control_readiness_handle = spawn_wallet_ingest_control_readiness_probe(
        ingest_control,
        serving_readiness.clone(),
        ingest_control_health_poll_interval,
        cancel.clone(),
    );
    let serving_runtime_drained = CancellationToken::new();
    let publisher_handle = pair_publisher.spawn(cancel.clone(), serving_runtime_drained.clone());
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
    let server = tonic::transport::Server::builder()
        .add_service(grpc_service)
        .add_service(reflection_service)
        .serve_with_shutdown(query_config.listen_addr, cancel.clone().cancelled_owned());
    supervise_query_runtime(
        server,
        cancel,
        &serving_readiness,
        QueryBackgroundTasks {
            serving_pair_publisher: publisher_handle,
            serving_runtime_drained,
            node_readiness_probe: node_readiness_handle,
            ingest_control_readiness_probe: ingest_control_readiness_handle,
            operations: ops_handle,
        },
    )
    .await
}

struct QueryBackgroundTasks {
    serving_pair_publisher: tokio::task::JoinHandle<()>,
    serving_runtime_drained: CancellationToken,
    node_readiness_probe: tokio::task::JoinHandle<()>,
    ingest_control_readiness_probe: tokio::task::JoinHandle<()>,
    operations: Option<OpsEndpointHandle>,
}

enum QueryRuntimeExit {
    ShutdownRequested,
    GrpcServer(Result<(), tonic::transport::Error>),
    ServingPairPublisher(Result<(), tokio::task::JoinError>),
    NodeReadinessProbe(Result<(), tokio::task::JoinError>),
    IngestControlReadinessProbe(Result<(), tokio::task::JoinError>),
    Operations(Result<(), OpsServerError>),
}

#[allow(
    clippy::too_many_lines,
    reason = "the explicit match keeps every supervised task's fatal-exit and drain ordering auditable"
)]
async fn supervise_query_runtime<Server>(
    server: Server,
    cancel: CancellationToken,
    readiness: &WalletServingReadiness,
    background_tasks: QueryBackgroundTasks,
) -> Result<(), QueryConfigError>
where
    Server: Future<Output = Result<(), tonic::transport::Error>>,
{
    let QueryBackgroundTasks {
        mut serving_pair_publisher,
        serving_runtime_drained,
        mut node_readiness_probe,
        mut ingest_control_readiness_probe,
        mut operations,
    } = background_tasks;
    tokio::pin!(server);
    let exit = tokio::select! {
        biased;
        () = cancel.cancelled() => QueryRuntimeExit::ShutdownRequested,
        server_outcome = &mut server => QueryRuntimeExit::GrpcServer(server_outcome),
        publisher_outcome = &mut serving_pair_publisher => {
            QueryRuntimeExit::ServingPairPublisher(publisher_outcome)
        }
        node_probe_outcome = &mut node_readiness_probe => {
            QueryRuntimeExit::NodeReadinessProbe(node_probe_outcome)
        }
        ingest_control_probe_outcome = &mut ingest_control_readiness_probe => {
            QueryRuntimeExit::IngestControlReadinessProbe(ingest_control_probe_outcome)
        }
        operations_outcome = wait_for_operations_exit(&mut operations) => {
            QueryRuntimeExit::Operations(operations_outcome)
        }
    };

    readiness.publish_shutting_down();
    cancel.cancel();
    let operations_shutdown = shutdown_operations(&mut operations).await;

    match exit {
        QueryRuntimeExit::ShutdownRequested => {
            let server_result = server.await;
            serving_runtime_drained.cancel();
            require_clean_task_shutdown("serving-pair publisher", serving_pair_publisher.await)?;
            require_clean_task_shutdown("node-readiness probe", node_readiness_probe.await)?;
            require_clean_task_shutdown(
                "ingest-control-readiness probe",
                ingest_control_readiness_probe.await,
            )?;
            operations_shutdown?;
            server_result.map_err(QueryConfigError::Transport)
        }
        QueryRuntimeExit::GrpcServer(server_result) => {
            serving_runtime_drained.cancel();
            require_clean_task_shutdown("serving-pair publisher", serving_pair_publisher.await)?;
            require_clean_task_shutdown("node-readiness probe", node_readiness_probe.await)?;
            require_clean_task_shutdown(
                "ingest-control-readiness probe",
                ingest_control_readiness_probe.await,
            )?;
            operations_shutdown?;
            server_result.map_err(QueryConfigError::Transport)?;
            Err(QueryConfigError::GrpcServerStopped)
        }
        QueryRuntimeExit::ServingPairPublisher(task_result) => {
            let task_error = unexpected_task_exit("serving-pair publisher", task_result);
            drain_server_after_runtime_failure(server.await);
            serving_runtime_drained.cancel();
            drain_task_after_runtime_failure("node-readiness probe", node_readiness_probe.await);
            drain_task_after_runtime_failure(
                "ingest-control-readiness probe",
                ingest_control_readiness_probe.await,
            );
            drain_operations_after_runtime_failure(operations_shutdown);
            Err(task_error)
        }
        QueryRuntimeExit::NodeReadinessProbe(task_result) => {
            let task_error = unexpected_task_exit("node-readiness probe", task_result);
            drain_server_after_runtime_failure(server.await);
            serving_runtime_drained.cancel();
            drain_task_after_runtime_failure(
                "serving-pair publisher",
                serving_pair_publisher.await,
            );
            drain_task_after_runtime_failure(
                "ingest-control-readiness probe",
                ingest_control_readiness_probe.await,
            );
            drain_operations_after_runtime_failure(operations_shutdown);
            Err(task_error)
        }
        QueryRuntimeExit::IngestControlReadinessProbe(task_result) => {
            let task_error = unexpected_task_exit("ingest-control-readiness probe", task_result);
            drain_server_after_runtime_failure(server.await);
            serving_runtime_drained.cancel();
            drain_task_after_runtime_failure(
                "serving-pair publisher",
                serving_pair_publisher.await,
            );
            drain_task_after_runtime_failure("node-readiness probe", node_readiness_probe.await);
            drain_operations_after_runtime_failure(operations_shutdown);
            Err(task_error)
        }
        QueryRuntimeExit::Operations(operations_result) => {
            let task_error = match operations_result {
                Ok(()) => QueryConfigError::RuntimeTaskStopped {
                    task: "operations endpoint",
                },
                Err(error) => QueryConfigError::Operations(error),
            };
            drain_server_after_runtime_failure(server.await);
            serving_runtime_drained.cancel();
            drain_task_after_runtime_failure(
                "serving-pair publisher",
                serving_pair_publisher.await,
            );
            drain_task_after_runtime_failure("node-readiness probe", node_readiness_probe.await);
            drain_task_after_runtime_failure(
                "ingest-control-readiness probe",
                ingest_control_readiness_probe.await,
            );
            drain_operations_after_runtime_failure(operations_shutdown);
            Err(task_error)
        }
    }
}

async fn wait_for_operations_exit(
    operations: &mut Option<OpsEndpointHandle>,
) -> Result<(), OpsServerError> {
    match operations {
        Some(handle) => handle.wait().await,
        None => std::future::pending().await,
    }
}

async fn shutdown_operations(
    operations: &mut Option<OpsEndpointHandle>,
) -> Result<(), QueryConfigError> {
    match operations.take() {
        Some(handle) => handle
            .shutdown()
            .await
            .map_err(QueryConfigError::Operations),
        None => Ok(()),
    }
}

fn require_clean_task_shutdown(
    task: &'static str,
    join_outcome: Result<(), tokio::task::JoinError>,
) -> Result<(), QueryConfigError> {
    join_outcome.map_err(|source| QueryConfigError::RuntimeTaskJoin { task, source })
}

fn unexpected_task_exit(
    task: &'static str,
    join_outcome: Result<(), tokio::task::JoinError>,
) -> QueryConfigError {
    match join_outcome {
        Ok(()) => QueryConfigError::RuntimeTaskStopped { task },
        Err(source) => QueryConfigError::RuntimeTaskJoin { task, source },
    }
}

fn drain_server_after_runtime_failure(server_outcome: Result<(), tonic::transport::Error>) {
    if let Err(error) = server_outcome {
        tracing::warn!(
            target: "zinder::query",
            event = "query_server_drain_failed",
            error = %error,
            "native query gRPC server failed while draining after a runtime task failure"
        );
    }
}

fn drain_task_after_runtime_failure(
    task: &'static str,
    join_outcome: Result<(), tokio::task::JoinError>,
) {
    if let Err(error) = join_outcome {
        tracing::warn!(
            target: "zinder::query",
            event = "query_runtime_task_drain_failed",
            task,
            error = %error,
            "wallet query runtime task failed while draining after another runtime failure"
        );
    }
}

fn drain_operations_after_runtime_failure(operations_outcome: Result<(), QueryConfigError>) {
    if let Err(error) = operations_outcome {
        tracing::warn!(
            target: "zinder::query",
            event = "query_operations_drain_failed",
            error = %error,
            "operations endpoint failed while draining after another runtime failure"
        );
    }
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
    use std::future;

    use super::*;
    use zinder_runtime::{ReadinessCause, ReadinessState};

    #[tokio::test]
    async fn serving_pair_publisher_exit_drains_the_runtime() {
        let runtime_readiness = Readiness::default();
        let serving_readiness =
            WalletServingReadiness::without_node_source(runtime_readiness.clone());
        let cancel = CancellationToken::new();
        let publisher = tokio::spawn(async {});
        let node_cancel = cancel.clone();
        let node_probe = tokio::spawn(async move {
            node_cancel.cancelled().await;
        });
        let ingest_control_cancel = cancel.clone();
        let ingest_control_probe = tokio::spawn(async move {
            ingest_control_cancel.cancelled().await;
        });
        let server_cancel = cancel.clone();
        let server = async move {
            server_cancel.cancelled().await;
            Ok(())
        };

        let runtime_outcome = supervise_query_runtime(
            server,
            cancel,
            &serving_readiness,
            QueryBackgroundTasks {
                serving_pair_publisher: publisher,
                serving_runtime_drained: CancellationToken::new(),
                node_readiness_probe: node_probe,
                ingest_control_readiness_probe: ingest_control_probe,
                operations: None,
            },
        )
        .await;

        assert!(matches!(
            runtime_outcome,
            Err(QueryConfigError::RuntimeTaskStopped {
                task: "serving-pair publisher"
            })
        ));
        assert!(matches!(
            runtime_readiness.report().cause,
            ReadinessCause::ShuttingDown
        ));
    }

    #[tokio::test]
    async fn requested_shutdown_drains_all_runtime_tasks_cleanly() {
        let runtime_readiness = Readiness::new(ReadinessState::ready(Some(1)));
        let serving_readiness =
            WalletServingReadiness::without_node_source(runtime_readiness.clone());
        let cancel = CancellationToken::new();
        let serving_runtime_drained = CancellationToken::new();
        let publisher_cancel = cancel.clone();
        let publisher_runtime_drained = serving_runtime_drained.clone();
        let publisher = tokio::spawn(async move {
            publisher_cancel.cancelled().await;
            publisher_runtime_drained.cancelled().await;
        });
        let node_cancel = cancel.clone();
        let node_probe = tokio::spawn(async move {
            node_cancel.cancelled().await;
        });
        let ingest_control_cancel = cancel.clone();
        let ingest_control_probe = tokio::spawn(async move {
            ingest_control_cancel.cancelled().await;
        });
        let server_cancel = cancel.clone();
        let server_runtime_drained = serving_runtime_drained.clone();
        let server = async move {
            server_cancel.cancelled().await;
            assert!(!server_runtime_drained.is_cancelled());
            Ok(())
        };
        cancel.cancel();

        let runtime_outcome = supervise_query_runtime(
            server,
            cancel,
            &serving_readiness,
            QueryBackgroundTasks {
                serving_pair_publisher: publisher,
                serving_runtime_drained,
                node_readiness_probe: node_probe,
                ingest_control_readiness_probe: ingest_control_probe,
                operations: None,
            },
        )
        .await;

        assert!(runtime_outcome.is_ok());
        assert!(matches!(
            runtime_readiness.report().cause,
            ReadinessCause::ShuttingDown
        ));
    }

    #[tokio::test]
    async fn node_probe_join_failure_is_a_typed_runtime_failure() {
        let runtime_readiness = Readiness::default();
        let serving_readiness =
            WalletServingReadiness::without_node_source(runtime_readiness.clone());
        let cancel = CancellationToken::new();
        let publisher_cancel = cancel.clone();
        let publisher = tokio::spawn(async move {
            publisher_cancel.cancelled().await;
        });
        let node_probe = tokio::spawn(future::pending::<()>());
        node_probe.abort();
        let ingest_control_cancel = cancel.clone();
        let ingest_control_probe = tokio::spawn(async move {
            ingest_control_cancel.cancelled().await;
        });
        let server_cancel = cancel.clone();
        let server = async move {
            server_cancel.cancelled().await;
            Ok(())
        };

        let runtime_outcome = supervise_query_runtime(
            server,
            cancel,
            &serving_readiness,
            QueryBackgroundTasks {
                serving_pair_publisher: publisher,
                serving_runtime_drained: CancellationToken::new(),
                node_readiness_probe: node_probe,
                ingest_control_readiness_probe: ingest_control_probe,
                operations: None,
            },
        )
        .await;

        assert!(matches!(
            runtime_outcome,
            Err(QueryConfigError::RuntimeTaskJoin {
                task: "node-readiness probe",
                ..
            })
        ));
        assert!(matches!(
            runtime_readiness.report().cause,
            ReadinessCause::ShuttingDown
        ));
    }

    #[tokio::test]
    async fn ingest_control_probe_join_failure_is_a_typed_runtime_failure() {
        let runtime_readiness = Readiness::default();
        let serving_readiness =
            WalletServingReadiness::without_node_source(runtime_readiness.clone());
        let cancel = CancellationToken::new();
        let publisher_cancel = cancel.clone();
        let publisher = tokio::spawn(async move {
            publisher_cancel.cancelled().await;
        });
        let node_cancel = cancel.clone();
        let node_probe = tokio::spawn(async move {
            node_cancel.cancelled().await;
        });
        let ingest_control_probe = tokio::spawn(future::pending::<()>());
        ingest_control_probe.abort();
        let server_cancel = cancel.clone();
        let server = async move {
            server_cancel.cancelled().await;
            Ok(())
        };

        let runtime_outcome = supervise_query_runtime(
            server,
            cancel,
            &serving_readiness,
            QueryBackgroundTasks {
                serving_pair_publisher: publisher,
                serving_runtime_drained: CancellationToken::new(),
                node_readiness_probe: node_probe,
                ingest_control_readiness_probe: ingest_control_probe,
                operations: None,
            },
        )
        .await;

        assert!(matches!(
            runtime_outcome,
            Err(QueryConfigError::RuntimeTaskJoin {
                task: "ingest-control-readiness probe",
                ..
            })
        ));
        assert!(matches!(
            runtime_readiness.report().cause,
            ReadinessCause::ShuttingDown
        ));
    }
}
