//! Process-wide tracing subscriber installation and `RUST_LOG` validation.

use std::{
    env::VarError,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use opentelemetry::{
    propagation::{Extractor, Injector, TextMapPropagator as _},
    trace::{TraceContextExt as _, TracerProvider as _},
};
use opentelemetry_otlp::{WithExportConfig as _, WithTonicConfig as _};
use opentelemetry_sdk::{
    Resource,
    propagation::TraceContextPropagator,
    trace::{BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider},
};
use serde::Deserialize;
use tonic::{
    metadata::MetadataMap,
    transport::{ClientTlsConfig, Endpoint, Server},
};
use tracing::{Instrument as _, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::{
    EnvFilter, Layer, filter::filter_fn, fmt, layer::SubscriberExt as _,
    util::SubscriberInitExt as _,
};

use crate::{ConfigLoader, ObservabilityConfig, RuntimeService};

const LOG_FILTER_ENV_VAR: &str = "RUST_LOG";
const DEFAULT_LOG_FILTER: &str = "info";
const TRACE_TARGET: &str = "zinder::trace";
const EXPORT_TIMEOUT: Duration = Duration::from_secs(2);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
static TRACE_EXPORT_ENABLED: AtomicBool = AtomicBool::new(false);

/// Error returned when the standard Zinder tracing subscriber cannot be
/// installed.
///
/// Binaries report it on stderr and exit non-zero before doing any work,
/// because no subscriber exists yet to log it.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TracingSubscriberError {
    /// `RUST_LOG` is set to a value that does not parse as a log filter.
    #[error("RUST_LOG={directives:?} is not a valid log filter: {reason}")]
    InvalidLogFilter {
        /// The rejected filter directives, as read from `RUST_LOG`.
        directives: String,
        /// Why the directives were rejected.
        reason: String,
    },
    /// `RUST_LOG` is set but is not valid Unicode.
    #[error("RUST_LOG is set but is not valid Unicode")]
    NonUnicodeLogFilter,
    /// The observability section could not be loaded.
    #[error(
        "invalid observability configuration. Check observability.otlp_endpoint and observability.sampling_rate"
    )]
    ObservabilityConfig,
    /// The OTLP gRPC exporter could not be initialized.
    #[error("cannot initialize the OTLP gRPC trace exporter")]
    TraceExporterInit,
    /// The process already has a global tracing subscriber or `log` logger.
    #[error("cannot install the global tracing subscriber: {reason}")]
    GlobalSubscriberInstall {
        /// Why installation failed.
        reason: String,
    },
}

/// Installs the standard Zinder tracing subscriber as the global default.
///
/// Reads the log filter from `RUST_LOG`, writes to stderr, and includes
/// target labels. An unset, empty, or whitespace-only `RUST_LOG` selects
/// `info`. Binaries call it once at startup.
///
/// # Errors
///
/// Returns [`TracingSubscriberError::InvalidLogFilter`] when `RUST_LOG` does
/// not parse as a log filter, [`TracingSubscriberError::NonUnicodeLogFilter`]
/// when it is not valid Unicode, and
/// [`TracingSubscriberError::GlobalSubscriberInstall`] when a global
/// subscriber is already installed.
pub fn install_tracing_subscriber() -> Result<(), TracingSubscriberError> {
    install_log_subscriber(log_filter()?)
}

/// Installs process logging and starts OTLP tracing when a collector is configured.
///
/// Call once inside the runtime. Await the returned handle's shutdown after serving
/// tasks have drained to export queued spans.
pub fn install_runtime_tracing_subscriber(
    service: RuntimeService,
    config_path: Option<&Path>,
) -> Result<RuntimeTracing, TracingSubscriberError> {
    let env_filter = log_filter()?;
    let config: ProcessObservability = ConfigLoader::new()
        .with_file(config_path.map(Path::to_path_buf))
        .with_zinder_env()
        .and_then(ConfigLoader::load)
        .map_err(|_| TracingSubscriberError::ObservabilityConfig)?;
    let provider = trace_provider(service.binary_name(), &config.observability)?;
    if let Some(provider) = provider.as_ref() {
        tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_target(true)
                    .with_filter(env_filter),
            )
            .with(otlp_layer(provider))
            .try_init()
            .map_err(|error| TracingSubscriberError::GlobalSubscriberInstall {
                reason: error.to_string(),
            })?;
        TRACE_EXPORT_ENABLED.store(true, Ordering::Relaxed);
    } else {
        install_log_subscriber(env_filter)?;
    }
    Ok(RuntimeTracing { provider })
}

fn otlp_layer<S>(provider: &SdkTracerProvider) -> impl Layer<S> + use<S>
where
    S: tracing::Subscriber + for<'scope> tracing_subscriber::registry::LookupSpan<'scope>,
{
    tracing_opentelemetry::layer()
        .with_tracer(provider.tracer("zinder"))
        .with_location(false)
        .with_threads(false)
        .with_filter(filter_fn(|metadata| {
            metadata.is_span() && metadata.target() == TRACE_TARGET
        }))
}

#[derive(Deserialize)]
struct ProcessObservability {
    #[serde(default)]
    observability: ObservabilityConfig,
}

/// Owns the OTLP trace provider for the lifetime of the serving process.
#[must_use = "await shutdown after serving tasks drain"]
pub struct RuntimeTracing {
    provider: Option<SdkTracerProvider>,
}

impl RuntimeTracing {
    /// Flushes queued spans and shuts down the provider with a bounded wait.
    pub async fn shutdown(self) {
        if let Some(provider) = self.provider {
            TRACE_EXPORT_ENABLED.store(false, Ordering::Relaxed);
            let stopped = tokio::task::spawn_blocking(move || {
                provider.shutdown_with_timeout(SHUTDOWN_TIMEOUT)
            })
            .await;
            if !matches!(stopped, Ok(Ok(()))) {
                tracing::warn!(target: "zinder::runtime", "OTLP trace export did not shut down cleanly");
            }
        }
    }
}

fn trace_provider(
    service_name: &'static str,
    observability: &ObservabilityConfig,
) -> Result<Option<SdkTracerProvider>, TracingSubscriberError> {
    let Some(uri) = observability.otlp_endpoint.clone() else {
        return Ok(None);
    };
    let tls = uri.scheme_str() == Some("https");
    let mut endpoint = Endpoint::from(uri)
        .connect_timeout(EXPORT_TIMEOUT)
        .timeout(EXPORT_TIMEOUT);
    if tls {
        endpoint = endpoint
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|_| TracingSubscriberError::TraceExporterInit)?;
    }
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_channel(endpoint.connect_lazy())
        .with_timeout(EXPORT_TIMEOUT)
        .build()
        .map_err(|_| TracingSubscriberError::TraceExporterInit)?;
    let processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_max_queue_size(2048)
                .with_max_export_batch_size(512)
                .with_scheduled_delay(EXPORT_TIMEOUT)
                .build(),
        )
        .build();
    let rate = observability.sampling_rate.unwrap_or_default();
    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(rate)));
    let resource = Resource::builder_empty()
        .with_service_name(service_name)
        .build();
    Ok(Some(
        SdkTracerProvider::builder()
            .with_sampler(sampler)
            .with_resource(resource)
            .with_span_processor(processor)
            .build(),
    ))
}

/// Enables W3C Trace Context extraction and request spans on the private ingest control listener.
#[must_use]
pub fn trace_ingest_control(server: Server) -> Server {
    if trace_export_enabled() {
        server.trace_fn(|request| ingest_control_span(request.headers()))
    } else {
        server
    }
}

fn ingest_control_span(headers: &http::HeaderMap) -> Span {
    let span = tracing::info_span!(target: TRACE_TARGET, parent: None, "ingest_control.request", otel.kind = "server");
    let parent = TraceContextPropagator::new().extract(&TraceHeaders(headers));
    if parent.has_active_span() {
        let _ = span.set_parent(parent);
    }
    span
}

pub(crate) fn trace_export_enabled() -> bool {
    TRACE_EXPORT_ENABLED.load(Ordering::Relaxed)
}

pub(crate) fn wallet_rpc_span(surface: &'static str, method: &'static str) -> Option<Span> {
    trace_export_enabled().then(|| {
        tracing::info_span!(
            target: TRACE_TARGET,
            parent: None,
            "wallet.request",
            otel.kind = "server",
            rpc.system = "grpc",
            rpc.service = surface,
            rpc.method = method,
        )
    })
}

/// Measures the upstream node call within the current trace, when sampled.
pub async fn trace_node_rpc<Output>(request: impl Future<Output = Output>) -> Output {
    if trace_export_enabled()
        && opentelemetry::Context::current()
            .span()
            .span_context()
            .is_sampled()
    {
        request
            .instrument(tracing::info_span!(
                target: TRACE_TARGET, "node.rpc", otel.kind = "client"
            ))
            .await
    } else {
        request.await
    }
}

pub(crate) fn inject_trace_context(metadata: &mut MetadataMap) {
    TraceContextPropagator::new().inject_context(
        &opentelemetry::Context::current(),
        &mut TraceMetadata(metadata),
    );
}

struct TraceHeaders<'a>(&'a http::HeaderMap);

impl Extractor for TraceHeaders<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0
            .get(key)?
            .to_str()
            .ok()
            .filter(|header| header.len() <= 512)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(http::HeaderName::as_str).collect()
    }
}

struct TraceMetadata<'a>(&'a mut MetadataMap);

impl Injector for TraceMetadata<'_> {
    fn set(&mut self, key: &str, header: String) {
        if let (Ok(key), Ok(header)) = (
            key.parse::<tonic::metadata::AsciiMetadataKey>(),
            header.parse::<tonic::metadata::AsciiMetadataValue>(),
        ) {
            self.0.insert(key, header);
        }
    }
}

fn log_filter() -> Result<EnvFilter, TracingSubscriberError> {
    let directives = match std::env::var(LOG_FILTER_ENV_VAR) {
        Ok(directives) => Some(directives),
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(_)) => return Err(TracingSubscriberError::NonUnicodeLogFilter),
    };
    log_filter_from_directives(directives.as_deref())
}

fn install_log_subscriber(env_filter: EnvFilter) -> Result<(), TracingSubscriberError> {
    fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .with_target(true)
        .try_init()
        .map_err(|error| TracingSubscriberError::GlobalSubscriberInstall {
            reason: error.to_string(),
        })
}

fn log_filter_from_directives(
    directives: Option<&str>,
) -> Result<EnvFilter, TracingSubscriberError> {
    let directives = directives
        .map(str::trim)
        .filter(|directives| !directives.is_empty())
        .unwrap_or(DEFAULT_LOG_FILTER);
    EnvFilter::builder().parse(directives).map_err(|error| {
        TracingSubscriberError::InvalidLogFilter {
            directives: directives.to_owned(),
            reason: error.to_string(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{TracingSubscriberError, log_filter_from_directives};

    #[test]
    fn unset_log_filter_defaults_to_info() {
        let filter = log_filter_from_directives(None);
        assert!(
            filter
                .as_ref()
                .is_ok_and(|filter| filter.to_string() == "info"),
            "{filter:?}"
        );
    }

    #[test]
    fn blank_log_filter_is_treated_as_unset() {
        for directives in ["", "   ", "\t\n"] {
            let filter = log_filter_from_directives(Some(directives));
            assert!(
                filter
                    .as_ref()
                    .is_ok_and(|filter| filter.to_string() == "info"),
                "{directives:?}: {filter:?}"
            );
        }
    }

    #[test]
    fn valid_log_filters_are_accepted() {
        for directives in [
            "debug",
            "info,zinder=debug",
            "warn,zinder::ingest=trace,tower=off",
            "zinder",
        ] {
            let filter = log_filter_from_directives(Some(directives));
            assert!(filter.is_ok(), "{directives:?}: {filter:?}");
        }
    }

    #[test]
    fn invalid_log_filters_are_rejected() {
        for directives in ["zinder=bogus", "info,zinder=verbose", "[unterminated", "{"] {
            let error = log_filter_from_directives(Some(directives)).err();
            assert!(
                matches!(
                    &error,
                    Some(TracingSubscriberError::InvalidLogFilter {
                        directives: rejected,
                        reason,
                    }) if rejected == directives && !reason.is_empty()
                ),
                "{directives:?}: {error:?}"
            );
        }
    }

    #[test]
    fn invalid_log_filter_error_names_the_variable_and_value() {
        let message = log_filter_from_directives(Some("zinder=bogus"))
            .err()
            .map_or_default(|error| error.to_string());
        assert!(
            message.starts_with("RUST_LOG=\"zinder=bogus\""),
            "{message}"
        );
        assert!(message.contains("not a valid log filter"), "{message}");
    }
}

#[cfg(test)]
mod export_tests {
    use opentelemetry_proto::tonic::collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::{TraceService, TraceServiceServer},
    };
    use tokio::{net::TcpListener, sync::mpsc};
    use tokio_stream::wrappers::TcpListenerStream;
    use tokio_util::sync::CancellationToken;
    use tonic::{Request, Response, Status, transport::Server};
    use zinder_core::Network;
    use zinder_proto::v1::ingest::{
        WriterStatusRequest, ingest_control_client::IngestControlClient,
        ingest_control_server::IngestControlServer,
    };
    use zinder_testkit::IngestControlFixtureService;

    use super::*;

    struct MockCollector(mpsc::Sender<ExportTraceServiceRequest>);

    #[tonic::async_trait]
    impl TraceService for MockCollector {
        async fn export(
            &self,
            request: Request<ExportTraceServiceRequest>,
        ) -> Result<Response<ExportTraceServiceResponse>, Status> {
            self.0
                .try_send(request.into_inner())
                .map_err(|_| Status::resource_exhausted("test collector queue is full"))?;
            Ok(Response::new(ExportTraceServiceResponse::default()))
        }
    }

    #[test]
    fn unset_endpoint_creates_no_provider_or_otlp_network_runtime() -> eyre::Result<()> {
        // This test has no Tokio runtime. Even a lazy tonic channel requires one.
        let config: ProcessObservability = ConfigLoader::new()
            .with_zinder_env_from_map([(
                "ZINDER_OBSERVABILITY__SAMPLING_RATE".to_owned(),
                "1".to_owned(),
            )])?
            .load()?;
        assert!(trace_provider("zinder-query", &config.observability)?.is_none());
        Ok(())
    }

    #[test]
    fn collector_endpoint_is_redacted_and_rejects_credentials() -> eyre::Result<()> {
        let configured: ObservabilityConfig =
            toml::from_str(r#"otlp_endpoint = "https://collector.internal:4317""#)?;
        let rendered = toml::to_string(&configured)?;
        assert!(rendered.contains("[REDACTED]"));
        assert!(!rendered.contains("collector.internal"));
        assert!(!format!("{configured:?}").contains("collector.internal"));
        for source in [
            r#"otlp_endpoint = "https://test:private-token@collector.internal:4317""#,
            r#"otlp_endpoint = "http://collector.internal/private-token""#,
            r#"otlp_endpoint = "http://collector.internal?token=private-token""#,
            "sampling_rate = 1.01",
            "sampling_rate = nan",
        ] {
            let error = toml::from_str::<ObservabilityConfig>(source)
                .err()
                .ok_or_else(|| eyre::eyre!("accepted invalid observability config"))?;
            // TOML diagnostics can include source text. Runtime loader errors and
            // Debug output must use only the validation message.
            assert!(!error.message().contains("private-token"));
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn otlp_flush_exports_correlated_wallet_and_ingest_control_spans() -> eyre::Result<()> {
        let shutdown = CancellationToken::new();
        let collector_listener = TcpListener::bind("127.0.0.1:0").await?;
        let collector_addr = collector_listener.local_addr()?;
        let (sender, mut exports) = mpsc::channel(4);
        let collector = tokio::spawn(
            Server::builder()
                .add_service(TraceServiceServer::new(MockCollector(sender)))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(collector_listener),
                    shutdown.clone().cancelled_owned(),
                ),
        );

        let config_file = tempfile::NamedTempFile::new()?;
        std::fs::write(
            config_file.path(),
            format!(
                "[observability]\notlp_endpoint = \"http://{collector_addr}\"\nsampling_rate = 1.0\n"
            ),
        )?;
        let tracing =
            install_runtime_tracing_subscriber(RuntimeService::Query, Some(config_file.path()))?;
        let control_listener = TcpListener::bind("127.0.0.1:0").await?;
        let control_addr = control_listener.local_addr()?;
        let control = tokio::spawn(
            trace_ingest_control(Server::builder())
                .add_service(IngestControlServer::new(IngestControlFixtureService::new(
                    Network::ZcashRegtest,
                )))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(control_listener),
                    shutdown.clone().cancelled_owned(),
                ),
        );

        let channel = crate::connect_zinder_grpc(&format!("http://{control_addr}"), None).await?;
        let mut client = IngestControlClient::new(channel);
        async {
            let _ = client.writer_status(WriterStatusRequest {}).await?;
            Ok::<(), Status>(())
        }
        .instrument(
            wallet_rpc_span("wallet_query", "ChainView")
                .ok_or_else(|| eyre::eyre!("trace export is disabled"))?,
        )
        .await?;
        drop(client);
        tracing.shutdown().await;

        let spans: Vec<_> = std::iter::from_fn(|| exports.try_recv().ok())
            .flat_map(|export| export.resource_spans)
            .flat_map(|resource| resource.scope_spans)
            .flat_map(|scope| scope.spans)
            .collect();
        let wallet_span = spans
            .iter()
            .find(|span| span.name == "wallet.request")
            .ok_or_else(|| eyre::eyre!("wallet span did not reach the collector"))?;
        let control_span = spans
            .iter()
            .find(|span| span.name == "ingest_control.request")
            .ok_or_else(|| eyre::eyre!("control span did not reach the collector"))?;
        assert_eq!(wallet_span.trace_id, control_span.trace_id);
        assert_eq!(wallet_span.span_id, control_span.parent_span_id);
        assert_eq!(wallet_span.parent_span_id, Vec::<u8>::new());

        shutdown.cancel();
        control.await??;
        collector.await??;
        Ok(())
    }
}
