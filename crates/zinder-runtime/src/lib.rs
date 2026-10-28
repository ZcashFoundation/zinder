//! Operational HTTP surface and shared configuration loader for Zinder service binaries.
//!
//! This crate owns:
//!
//! - The typed [`Readiness`] state machine and [`ReadinessCause`]
//!   values defined in `docs/architecture/service-operations.md`.
//! - The `/healthz`, `/readyz`, and `/metrics` HTTP endpoints
//!   ([`spawn_ops_endpoint`], [`serve_ops_endpoint`]).
//! - The shared configuration error type, fluent layered loader, and shared
//!   schema/redacted-render mirrors ([`ConfigError`], [`ConfigLoader`],
//!   [`NetworkSection`], [`NetworkToml`], [`NodeAuthToml`], [`NodeHealthToml`],
//!   [`NodeToml`], [`zinder_environment_source`], [`require_field`],
//!   [`duration_as_millis_u64`]) that every service binary uses to honor the
//!   `defaults -> file -> ZINDER_* env -> CLI overrides` precedence.
//! - Two thin lifecycle helpers used by every binary entry point:
//!   [`cancel_on_terminating_signal`] and [`install_tracing_subscriber`].
//! - The process-wide Prometheus metrics recorder
//!   ([`install_metrics_recorder`], [`install_metrics_recorder_for_service`]).
//!
//! It deliberately exposes no domain types.

mod auth;
mod bind_guard;
mod config;
mod env_diagnostics;
mod env_var_docs;
mod memory_budget;
mod metrics;
mod ops_endpoint;
mod readiness;
mod rpc_metrics;
mod sections;
mod startup_phase;
mod transport;

pub use auth::{
    AuthenticatedChannel, BearerToken, BearerTokenClientInterceptor, BearerTokenConnectError,
    BearerTokenError, BearerTokenServerInterceptor, bearer_metadata,
};
pub use bind_guard::{
    BindAddressClass, classify_bind_address, guard_optional_serving_bind, guard_serving_bind,
};
pub use config::{
    ConfigError, ConfigLoader, NetworkSection, NetworkToml, NodeAuthToml, NodeHealthToml, NodeToml,
    ZinderEnvironmentSource, duration_as_millis_u64, load_bearer_token, parse_socket_addr,
    require_field, zinder_environment_source, zinder_environment_source_from_map,
};
pub use env_var_docs::{
    ENVIRONMENT_VARIABLES, EnvVarDoc, Requirement as EnvVarRequirement,
    render_environment_variable_table,
};
pub use memory_budget::{
    canonical_reader_block_cache_bytes, canonical_reader_max_open_files,
    container_memory_budget_bytes,
};
pub use metrics::{
    MetricsHandle, MetricsInstallError, install_metrics_recorder,
    install_metrics_recorder_for_service,
};
pub use ops_endpoint::{
    OpsEndpointHandle, OpsServer, OpsServerError, serve_ops_endpoint, spawn_ops_endpoint,
    spawn_ops_endpoint_for,
};
pub use readiness::{
    IngestPhase, NodeUnavailableDetail, Readiness, ReadinessCause, ReadinessReport, ReadinessState,
    TrafficReadinessInterceptor, UpstreamHealth, UpstreamNotReadyDetail, WriterAttestation,
    not_ready_status,
};
pub use rpc_metrics::{RpcMetricNames, RpcOutcome, describe_rpc_metrics, record_rpc_request};
pub use sections::{
    CanonicalSecondaryStorageSection, CanonicalSecondaryStorageToml, DEFAULT_ALLOW_PUBLIC_BIND,
    IngestControlReaderToml, IngestControlSection, IngestControlWriterToml, OpsSection, OpsToml,
    ProjectorControlSection, ProjectorControlToml, ResolvedCanonicalSecondaryStorage,
    ResolvedIngestControlReader, ResolvedIngestControlWriter, ResolvedProjectorControl,
    ResolvedRetention, ResolvedSecondaryStorage, RetentionSection, RetentionToml,
    RocksDbResourceBudgetSection, RocksDbResourceBudgetToml, RuntimeService,
    SecondaryStorageSection, SecondaryStorageToml, SecuritySection, SecurityToml,
    StorageRoleSection, StorageRoleToml, defaults as section_defaults, resolve_allow_public_bind,
    resolve_canonical_reader_rocksdb_budget, resolve_canonical_secondary_storage,
    resolve_canonical_writer_rocksdb_budget, resolve_ingest_control_reader,
    resolve_ingest_control_writer, resolve_materialized_view_reader_rocksdb_budget,
    resolve_materialized_view_writer_rocksdb_budget, resolve_ops_listen_addr,
    resolve_projector_control, resolve_retention, resolve_secondary_storage,
    resolve_wallet_projection_reader_rocksdb_budget,
    resolve_wallet_projection_writer_rocksdb_budget,
};
pub use startup_phase::{StartupPhase, StartupPhaseGuard};
pub use transport::{
    InvalidZinderGrpcEndpoint, connect_zinder_grpc, validate_zinder_grpc_endpoint,
};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Upper bound on the size of an incoming gRPC request frame, applied to
/// every serving Zinder gRPC server via `max_decoding_message_size`.
///
/// The value accommodates the largest legitimate request: a broadcast
/// transaction, itself capped at
/// [`zinder_core::MAX_RAW_TRANSACTION_BYTES`] (2 MB), plus gRPC framing
/// overhead. It must stay at or above that 2 MB bound so an oversized
/// broadcast is rejected by the application-layer broadcast guard with its
/// typed error, not by the transport.
///
/// Setting it explicitly pins tonic's implicit default in place so a
/// future framework default change cannot silently widen the limit.
pub const MAX_DECODING_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

/// Git commit embedded by release builds.
///
/// Local builds outside the release workflow use `unknown` rather than an
/// empty value so operators can distinguish absent provenance explicitly.
pub const BUILD_GIT_COMMIT: &str = match option_env!("ZINDER_BUILD_GIT_COMMIT") {
    Some(commit) => commit,
    None => "unknown",
};

/// Spawns a task that cancels `cancel` when the process receives a
/// terminating signal. Returns the join handle for the spawned task;
/// callers usually drop it.
///
/// Used by every Zinder binary so the same shutdown semantics apply
/// regardless of which service is running.
///
/// Cancels on both `SIGINT` (`Ctrl-C` at a terminal) and `SIGTERM`
/// (`docker stop`, `kubectl delete pod`, `systemctl stop`). Without the
/// `SIGTERM` branch, Docker waits the stop-timeout, sends `SIGKILL`, and
/// the binary never gets a chance to drop its `RocksDB` handle. A hard
/// kill leaves the WAL un-flushed; the next start has to replay the
/// stranded writes from disk, which is the entry condition for the
/// bulk-catchup OOM trap recorded in
/// [the resource-tuning runbook](../../../docs/runbooks/bulk-catchup-resource-tuning.md).
#[must_use = "drop the handle to detach the task or await it for symmetric shutdown"]
pub fn cancel_on_terminating_signal(cancel: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        await_terminating_signal().await;
        cancel.cancel();
    })
}

#[cfg(unix)]
async fn await_terminating_signal() {
    use tokio::signal::unix::{Signal, SignalKind, signal};

    fn install(kind: SignalKind, name: &'static str) -> Option<Signal> {
        match signal(kind) {
            Ok(stream) => Some(stream),
            Err(error) => {
                tracing::warn!(
                    target: "zinder::runtime",
                    %error,
                    signal = name,
                    "failed to install terminating-signal handler"
                );
                None
            }
        }
    }

    let interrupt = install(SignalKind::interrupt(), "SIGINT");
    let terminate = install(SignalKind::terminate(), "SIGTERM");

    match (interrupt, terminate) {
        (Some(mut interrupt), Some(mut terminate)) => {
            tokio::select! {
                _ = interrupt.recv() => {}
                _ = terminate.recv() => {}
            }
        }
        (Some(mut interrupt), None) => {
            let _ = interrupt.recv().await;
        }
        (None, Some(mut terminate)) => {
            let _ = terminate.recv().await;
        }
        (None, None) => {}
    }
}

#[cfg(not(unix))]
async fn await_terminating_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// Installs the standard Zinder tracing subscriber as the global default.
///
/// Reads `RUST_LOG` if present, defaults to `info`, writes to stderr, and
/// includes target labels. Idempotent in practice because tracing rejects
/// repeated `init()` calls; binaries should call it once at startup.
pub fn install_tracing_subscriber() {
    use tracing_subscriber::{EnvFilter, fmt};

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .with_target(true)
        .init();
}

/// Verifies the host CPU provides every instruction-set feature this binary
/// was compiled to require, logging the shortfall and returning `false` when
/// it does not.
///
/// Release images compile with `-C target-cpu=x86-64-v3`, which lets both the
/// Rust code and the vendored `RocksDB` C++ emit AVX2/BMI2 instructions
/// unconditionally. A host below that baseline dies with `SIGILL` deep inside
/// storage code, so binaries call this before opening storage and exit with a
/// readable error instead. Builds without the flag (tests, local dev, non-x86
/// images) require nothing and always pass.
#[must_use]
pub fn host_cpu_meets_compiled_baseline() -> bool {
    let missing = missing_required_cpu_features();
    if missing.is_empty() {
        return true;
    }
    tracing::error!(
        target: "zinder::runtime",
        event = "host_cpu_below_compiled_baseline",
        missing = missing.join(", "),
        "this build requires x86-64-v3; refusing to start on this host"
    );
    false
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
fn missing_required_cpu_features() -> Vec<&'static str> {
    let required = [
        ("avx2", std::arch::is_x86_feature_detected!("avx2")),
        ("bmi1", std::arch::is_x86_feature_detected!("bmi1")),
        ("bmi2", std::arch::is_x86_feature_detected!("bmi2")),
        ("fma", std::arch::is_x86_feature_detected!("fma")),
        ("lzcnt", std::arch::is_x86_feature_detected!("lzcnt")),
        (
            "pclmulqdq",
            std::arch::is_x86_feature_detected!("pclmulqdq"),
        ),
    ];
    required
        .into_iter()
        .filter(|(_, detected)| !detected)
        .map(|(feature, _)| feature)
        .collect()
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
fn missing_required_cpu_features() -> Vec<&'static str> {
    Vec::new()
}
