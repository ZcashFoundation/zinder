//! Cipherscan-compatible REST adapter entry point.

use std::{net::SocketAddr, path::PathBuf, process::ExitCode, sync::Arc};

use clap::Parser;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use zinder_compat_cipherscan::CipherscanRestAdapter;
use zinder_core::wire::encode_zinder_native_chain_name;
use zinder_runtime::{
    HttpListenerLimits, OpsEndpointHandle, Readiness, ReadinessCause, ReadinessState,
    RuntimeService, StartupPhase, TracingSubscriberError, cancel_on_terminating_signal,
    connect_zinder_grpc_at_startup, install_tracing_subscriber, serve_http, spawn_ops_endpoint_for,
};
use zinder_source::redact_endpoint_url;

mod config;

use config::{CipherscanConfig, CipherscanConfigError, CipherscanConfigOverrides};

#[global_allocator]
static GLOBAL_ALLOCATOR: zinder_runtime::RuntimeAllocator = zinder_runtime::RuntimeAllocator;

#[derive(Parser)]
#[command(name = "zinder-compat-cipherscan")]
#[command(about = "Cipherscan-compatible REST adapter over Zinder native gRPC services")]
#[command(version)]
struct Cli {
    /// TOML configuration file loaded before environment variables and CLI overrides.
    #[arg(long = "config", global = true)]
    config_path: Option<PathBuf>,
    /// Print the resolved command configuration without opening network listeners.
    #[arg(long = "print-config", global = true)]
    print_config: bool,
    /// Network name, such as zcash-regtest.
    #[arg(long)]
    network: Option<String>,
    /// Cipherscan-compatible REST listen address, such as 127.0.0.1:9070.
    #[arg(long = "listen-addr")]
    listen_addr: Option<SocketAddr>,
    /// Operational HTTP endpoint listen address for /healthz, /readyz, /metrics.
    #[arg(long = "ops-listen-addr")]
    ops_listen_addr: Option<SocketAddr>,
    /// Zinder `ExplorerQuery` gRPC endpoint backing Cipherscan REST reads.
    #[arg(long = "explorer-query-endpoint")]
    explorer_query_endpoint: Option<String>,
    /// Zinder `WalletQuery` gRPC endpoint backing Cipherscan REST reads and broadcast.
    #[arg(long = "wallet-query-endpoint")]
    wallet_query_endpoint: Option<String>,
    /// External endpoint serving the current ZEC/USD market price.
    #[arg(long = "current-price-endpoint")]
    current_price_endpoint: Option<String>,
    /// External historical ZEC/USD endpoint template containing `{date}`.
    #[arg(long = "historical-price-endpoint-template")]
    historical_price_endpoint_template: Option<String>,
    /// Path to a file containing the shared-secret bearer token used by Zinder gRPC services.
    #[arg(long = "bearer-token-path")]
    bearer_token_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
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
    let render_result = config::load_cipherscan_config(config_path, cli.into())
        .and_then(|cipherscan_config| config::cipherscan_config_toml(&cipherscan_config));

    match render_result {
        Ok(rendered_toml) => {
            println!("{rendered_toml}");
            ExitCode::SUCCESS
        }
        Err(error) => emit_runtime_error(&error),
    }
}

async fn run_runtime(cli: Cli) -> ExitCode {
    match run_cipherscan_adapter(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => emit_runtime_error(&error),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "adapter startup and shutdown stay together so listener admission and operational endpoint error precedence remain auditable"
)]
async fn run_cipherscan_adapter(cli: Cli) -> Result<(), CipherscanConfigError> {
    let load_config_phase = StartupPhase::LoadConfig.start();
    let config_path = cli.config_path.clone();
    let cipherscan_config = match config::load_cipherscan_config(config_path, cli.into()) {
        Ok(config) => {
            load_config_phase.complete();
            config
        }
        Err(error) => {
            load_config_phase.fail(&error);
            return Err(error);
        }
    };

    let readiness = Readiness::default();
    readiness.set(ReadinessState::starting());
    let start_api_phase = StartupPhase::StartApi.start();
    let ops_handle = spawn_ops_endpoint_for(
        RuntimeService::CompatCipherscan,
        cipherscan_config.ops_listen_addr,
        env!("CARGO_PKG_VERSION"),
        encode_zinder_native_chain_name(cipherscan_config.network),
        readiness.clone(),
        Arc::from([]),
    )
    .await?;

    let cancel = CancellationToken::new();
    let _signal_handle = cancel_on_terminating_signal(cancel.clone());
    let Some(explorer_channel) = connect_zinder_grpc_at_startup(
        &cipherscan_config.explorer_query_endpoint,
        cipherscan_config.bearer_token.as_ref(),
        &cancel,
    )
    .await?
    else {
        return shut_down_ops_endpoint(ops_handle).await;
    };
    let Some(wallet_channel) = connect_zinder_grpc_at_startup(
        &cipherscan_config.wallet_query_endpoint,
        cipherscan_config.bearer_token.as_ref(),
        &cancel,
    )
    .await?
    else {
        return shut_down_ops_endpoint(ops_handle).await;
    };
    let listener = TcpListener::bind(cipherscan_config.listen_addr)
        .await
        .map_err(|source| CipherscanConfigError::Bind {
            listen_addr: cipherscan_config.listen_addr,
            source,
        })?;

    let adapter = CipherscanRestAdapter::new(
        cipherscan_config.network,
        explorer_channel,
        wallet_channel,
        cipherscan_config.market_price_endpoints.clone(),
        cancel.clone(),
    )?;
    let app = adapter.clone().router();

    start_api_phase.complete();
    StartupPhase::Ready.start().complete();
    readiness.set(ReadinessState::ready(None));

    log_adapter_started(&cipherscan_config);

    let serve_result = serve_http(
        listener,
        app,
        HttpListenerLimits::default(),
        drain_readiness_on_cancel(cancel.clone(), readiness),
    )
    .await;
    adapter.shutdown_realtime().await;

    tracing::info!(
        target: "zinder::compat::cipherscan",
        event = "cipherscan_adapter_stopped",
        "Cipherscan-compatible REST adapter stopped"
    );

    let ops_shutdown_result = shut_down_ops_endpoint(ops_handle).await;

    match serve_result {
        Err(error) => {
            if let Err(ops_error) = ops_shutdown_result {
                tracing::warn!(
                    target: "zinder::compat::cipherscan",
                    event = "ops_endpoint_shutdown_failed",
                    error = %ops_error,
                    "operational endpoint shutdown also failed"
                );
            }
            Err(CipherscanConfigError::Serve(error))
        }
        Ok(()) => ops_shutdown_result,
    }
}

/// Logs the adapter's start. Endpoint URLs are redacted, because their user
/// information or query may carry a credential.
fn log_adapter_started(cipherscan_config: &CipherscanConfig) {
    let market_price_endpoints = &cipherscan_config.market_price_endpoints;
    tracing::info!(
        target: "zinder::compat::cipherscan",
        event = "cipherscan_adapter_started",
        network = encode_zinder_native_chain_name(cipherscan_config.network),
        listen_addr = %cipherscan_config.listen_addr,
        explorer_query_endpoint = %redact_endpoint_url(&cipherscan_config.explorer_query_endpoint),
        wallet_query_endpoint = %redact_endpoint_url(&cipherscan_config.wallet_query_endpoint),
        current_price_endpoint = %redact_endpoint_url(market_price_endpoints.current.as_str()),
        historical_price_endpoint_template = %redact_endpoint_url(&market_price_endpoints.historical_template),
        "Cipherscan-compatible REST adapter started"
    );
}

async fn shut_down_ops_endpoint(
    ops_handle: Option<OpsEndpointHandle>,
) -> Result<(), CipherscanConfigError> {
    match ops_handle {
        Some(handle) => handle.shutdown().await.map_err(CipherscanConfigError::from),
        None => Ok(()),
    }
}

async fn drain_readiness_on_cancel(cancel: CancellationToken, readiness: Readiness) {
    cancel.cancelled().await;
    readiness.set(ReadinessState::not_ready(ReadinessCause::ShuttingDown));
}

#[allow(
    clippy::print_stderr,
    reason = "no tracing subscriber is installed, so stderr is the only channel left"
)]
fn emit_tracing_subscriber_error(error: &TracingSubscriberError) -> ExitCode {
    eprintln!("{}: {error}", env!("CARGO_BIN_NAME"));
    ExitCode::FAILURE
}

fn emit_runtime_error(error: &CipherscanConfigError) -> ExitCode {
    tracing::error!(
        target: "zinder::compat::cipherscan",
        event = "cipherscan_adapter_run_failed",
        error = %error,
        "Cipherscan-compatible REST adapter failed"
    );
    ExitCode::FAILURE
}

impl From<Cli> for CipherscanConfigOverrides {
    fn from(cli: Cli) -> Self {
        Self {
            network: cli.network,
            listen_addr: cli.listen_addr,
            ops_listen_addr: cli.ops_listen_addr,
            explorer_query_endpoint: cli.explorer_query_endpoint,
            wallet_query_endpoint: cli.wallet_query_endpoint,
            current_price_endpoint: cli.current_price_endpoint,
            historical_price_endpoint_template: cli.historical_price_endpoint_template,
            bearer_token_path: cli.bearer_token_path,
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::sync::CancellationToken;
    use zinder_compat_cipherscan::CipherscanMarketPriceEndpoints;
    use zinder_core::Network;
    use zinder_runtime::{Readiness, ReadinessCause, ReadinessState};
    use zinder_testkit::log_capture::LogCapture;

    use super::{CipherscanConfig, drain_readiness_on_cancel, log_adapter_started};

    #[test]
    fn the_started_event_redacts_endpoint_credentials() -> Result<(), Box<dyn std::error::Error>> {
        let cipherscan_config = CipherscanConfig {
            network: Network::ZcashRegtest,
            listen_addr: "127.0.0.1:9070".parse()?,
            ops_listen_addr: None,
            allow_public_bind: false,
            explorer_query_endpoint: "http://reader:hunter2@127.0.0.1:9068/?token=s3cret"
                .to_owned(),
            wallet_query_endpoint: "http://reader:hunter2@127.0.0.1:9102/?token=s3cret".to_owned(),
            market_price_endpoints: CipherscanMarketPriceEndpoints::new(
                reqwest::Url::parse("https://prices.test/simple?api_key=s3cret")?,
                "https://prices.test/history?date={date}&api_key=s3cret".to_owned(),
            )?,
            bearer_token_path: None,
            bearer_token: None,
        };
        let capture = LogCapture::install_for_target("zinder::compat::cipherscan");

        log_adapter_started(&cipherscan_config);

        let events = capture.events();
        let started = events
            .iter()
            .find(|event| event.field("event") == Some("cipherscan_adapter_started"))
            .ok_or("the started event must be logged")?;
        for (field, logged) in &started.fields {
            assert!(
                !logged.contains("hunter2") && !logged.contains("s3cret"),
                "{field} carries a credential: {logged}"
            );
        }
        assert!(
            started
                .field("explorer_query_endpoint")
                .is_some_and(|endpoint| endpoint.contains("127.0.0.1:9068")),
            "{started:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_drains_readiness_before_server_shutdown() {
        let readiness = Readiness::new(ReadinessState::ready(None));
        let cancel = CancellationToken::new();
        cancel.cancel();

        drain_readiness_on_cancel(cancel, readiness.clone()).await;

        let report = readiness.report();
        assert_eq!(report.cause, ReadinessCause::ShuttingDown);
        assert!(!report.is_ready);
    }
}
