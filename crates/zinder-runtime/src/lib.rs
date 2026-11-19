//! Operational HTTP surface and shared configuration loader for Zinder service binaries.
//!
//! This crate owns:
//!
//! - The typed [`Readiness`] state machine and [`ReadinessCause`]
//!   values defined in `docs/reference/readiness.md`, plus the
//!   [`ComposedReadiness`] precedence shared by multi-input runtimes.
//! - The `/healthz`, `/readyz`, and `/metrics` HTTP endpoints
//!   ([`spawn_ops_endpoint`], [`serve_ops_endpoint`]), and the `healthcheck`
//!   subcommand that probes them from inside a container
//!   ([`OpsHealthcheckArgs`], [`probe_ops_endpoint`]).
//! - The bounded HTTP/1 listener ([`serve_http`], [`HttpListenerLimits`]) that
//!   serves every axum router, so header, body, idle, and connection limits
//!   are applied in one place.
//! - The shared configuration error type, fluent layered loader, and shared
//!   schema/redacted-render mirrors ([`ConfigError`], [`ConfigLoader`],
//!   [`NetworkSection`], [`NetworkToml`], [`NodeAuthToml`], [`NodeHealthToml`],
//!   [`NodeIndexerTlsToml`], [`NodeToml`], [`NodeFallbacksToml`], [`zinder_environment_source`],
//!   [`require_field`], [`duration_as_millis_u64`]) that every service binary
//!   uses to honor the
//!   `defaults -> file -> ZINDER_* env -> CLI overrides` precedence.
//! - Two thin lifecycle helpers used by every binary entry point:
//!   [`cancel_on_terminating_signal`] and [`install_tracing_subscriber`],
//!   which rejects an invalid `RUST_LOG` with [`TracingSubscriberError`].
//! - The gRPC server builder every public listener serves with
//!   ([`public_grpc_server`]), which bounds streams per connection, pings
//!   idle connections, limits how many requests the process receives at
//!   once ([`GrpcReceiveLimitLayer`]), and ends responses whose client stops
//!   reading them ([`GrpcSendStallLayer`]).
//! - The supervision of a wallet-serving runtime ([`SupervisedRuntime`],
//!   [`SupervisedTasks`]): its gRPC listener, which serves the wallet
//!   service behind the readiness gate with gRPC reflection and
//!   `grpc.health.v1` outside it, and its background tasks, which stop
//!   together.
//! - The process allocator every binary entry point installs
//!   ([`RuntimeAllocator`]).
//! - The process-wide Prometheus metrics recorder
//!   ([`install_metrics_recorder`], [`install_metrics_recorder_for_service`]).
//! - Canonical secondary attestation against `CanonicalControl.WriterStatus`
//!   ([`classify_canonical_writer_fence`],
//!   [`converge_on_canonical_writer_fence`], [`WriterAttestation`]), shared
//!   by every runtime that reads a canonical `RocksDB` secondary.
//! - Pre-decode element-count bounds on repeated gRPC request fields
//!   ([`RepeatedFieldGuard`], [`MethodFieldLimits`], [`RepeatedFieldLimit`]),
//!   applied by every public gRPC server whose requests carry lists.
//!
//! It deliberately defines no storage or protocol types of its own; it
//! composes those owned by `zinder-core`, `zinder-proto`, and `zinder-store`.

mod allocator;
mod auth;
mod bind_guard;
mod canonical_fence_attestation;
mod composed_readiness;
mod config;
mod env_diagnostics;
mod env_var_docs;
mod http_listener;
mod logging;
mod memory_budget;
mod metrics;
mod ops_endpoint;
mod ops_healthcheck;
#[cfg(test)]
mod perf;
mod public_grpc_health;
mod public_grpc_server;
mod readiness;
mod repeated_field_limits;
mod rpc_metrics;
mod runtime_supervision;
mod sections;
mod startup_phase;
mod transport;

pub use allocator::RuntimeAllocator;
pub use auth::{
    AuthenticatedChannel, BearerToken, BearerTokenClientInterceptor, BearerTokenConnectError,
    BearerTokenError, BearerTokenServerInterceptor, bearer_metadata,
};
pub use bind_guard::{
    BindAddressClass, classify_bind_address, guard_optional_serving_bind, guard_serving_bind,
};
pub use canonical_fence_attestation::{
    CanonicalFenceConvergence, CanonicalFenceConvergenceError, CanonicalFenceConvergencePolicy,
    CanonicalFenceDivergence, CanonicalFenceLag, CanonicalFenceProbe, CanonicalFenceRelation,
    WriterAttestation, classify_canonical_writer_fence, compare_canonical_writer_fences,
    converge_on_canonical_writer_fence, expected_writer_fence,
    writer_attests_network_upgrade_schedule,
};
pub use composed_readiness::{ComposedReadiness, ReadinessInput};
pub use config::{
    ConfigError, ConfigLoader, NetworkSection, NetworkToml, NodeAuthToml, NodeFallbacksToml,
    NodeHealthToml, NodeIndexerTlsToml, NodeToml, ZinderEnvironmentSource, duration_as_millis_u64,
    load_bearer_token, parse_socket_addr, require_field, zinder_environment_source,
    zinder_environment_source_from_map,
};
pub use env_var_docs::{
    ENVIRONMENT_VARIABLES, EnvVarDoc, Requirement as EnvVarRequirement,
    render_environment_variable_table,
};
pub use http_listener::{
    DEFAULT_HTTP_HEADER_READ_TIMEOUT, DEFAULT_HTTP_MAX_CONNECTIONS,
    DEFAULT_HTTP_MAX_REQUEST_BODY_BYTES, DEFAULT_HTTP_REQUEST_BODY_READ_TIMEOUT,
    DEFAULT_HTTP_WRITE_STALL_TIMEOUT, HttpListenerLimits, serve_http,
};
pub use logging::{TracingSubscriberError, install_tracing_subscriber};
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
pub use ops_healthcheck::{
    DEFAULT_OPS_HEALTHCHECK_PATH, OPS_HEALTHCHECK_TIMEOUT, OPS_LISTEN_ADDR_ENV_VAR,
    OpsHealthcheckArgs, OpsHealthcheckError, config_file_ops_listen_addr, probe_ops_endpoint,
    resolve_ops_healthcheck_addr,
};
pub use public_grpc_server::{
    DEFAULT_GRPC_RECEIVE_SLOTS, DEFAULT_GRPC_RECEIVE_TIMEOUT, DEFAULT_GRPC_SEND_STALL_TIMEOUT,
    GrpcCallTermination, GrpcCallTerminationLayer, GrpcReceiveLimit, GrpcReceiveLimitLayer,
    GrpcReceiveLimits, GrpcSendStall, GrpcSendStallLayer, MAX_CONCURRENT_STREAMS_PER_CONNECTION,
    PublicGrpcServer, public_grpc_reflection, public_grpc_server, public_grpc_server_with,
};
pub use readiness::{
    ActiveNodeEndpoint, IngestPhase, NodeUnavailableDetail, Readiness, ReadinessCause,
    ReadinessDependency, ReadinessReport, ReadinessState, TrafficReadinessInterceptor,
    UpstreamHealth, UpstreamNotReadyDetail, not_ready_status,
};
pub use repeated_field_limits::{
    MethodFieldLimits, RepeatedElementEncoding, RepeatedFieldGuard, RepeatedFieldLimit,
};
pub use rpc_metrics::{RpcMetricNames, RpcOutcome, describe_rpc_metrics, record_rpc_request};
pub use runtime_supervision::{
    GRPC_SERVER_CLOSE_TIMEOUT, GRPC_SERVER_DRAIN_TIMEOUT, RuntimeSupervisionError,
    RuntimeTaskFailure, ShutdownRequest, SupervisedGrpcRouter, SupervisedRuntime, SupervisedTasks,
};
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
    DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT, InvalidZinderGrpcEndpoint, connect_zinder_grpc,
    connect_zinder_grpc_with_timeout, validate_zinder_grpc_endpoint,
};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Upper bound on the size of an incoming gRPC request message, applied to
/// every serving Zinder gRPC server via `max_decoding_message_size`.
///
/// tonic holds a request message in memory while it decodes it, so this
/// bound times [`DEFAULT_GRPC_RECEIVE_SLOTS`] bounds the request bytes a
/// public server decodes at once.
///
/// The largest legitimate request is a broadcast transaction of
/// [`zinder_core::MAX_RAW_TRANSACTION_BYTES`] (2,000,000 bytes), which
/// encodes to at most 2,000,015 bytes as a lightwalletd `RawTransaction` and
/// fits under 2 MiB with room to spare. A larger transaction that still fits
/// is rejected by the broadcast guard with its typed error; one beyond this
/// bound fails in the transport with `OUT_OF_RANGE`, before tonic buffers
/// it.
///
/// Setting it explicitly also keeps a future tonic default from silently
/// widening the limit.
pub const MAX_DECODING_MESSAGE_BYTES: usize = 2 * 1024 * 1024;

/// Pre-decode element bound for a repeated enum request field that no
/// handler caps, applied through [`RepeatedFieldGuard`].
///
/// Such a field selects among a handful of enum values, so no client needs
/// more elements than this; the bound only stops a request that repeats them
/// to inflate decoding.
pub const MAX_UNCAPPED_ENUM_LIST_ELEMENTS: usize = 1_000;

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
