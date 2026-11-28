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
mod browser_transport;
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
mod process;
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
pub use browser_transport::{BrowserOrigins, is_grpc_web_request};
pub use canonical_fence_attestation::{
    CanonicalFenceConvergence, CanonicalFenceConvergenceError, CanonicalFenceConvergencePolicy,
    CanonicalFenceDivergence, CanonicalFenceLag, CanonicalFenceProbe, CanonicalFenceRelation,
    StaleDeadline, WriterAttestation, classify_canonical_writer_fence,
    compare_canonical_writer_fences, converge_on_canonical_writer_fence, expected_writer_fence,
    refresh_within_stale_deadline, writer_attests_network_upgrade_schedule,
};
pub use composed_readiness::{ComposedReadiness, ReadinessInput};
pub use config::{
    ConfigError, ConfigLoader, NetworkSection, NetworkToml, NodeAuthToml, NodeFallbacksToml,
    NodeHealthToml, NodeIndexerTlsToml, NodeToml, ZinderEnvironmentSource, duration_as_millis_u64,
    load_bearer_token, parse_socket_addr, require_field, resolve_optional_bearer_token,
    zinder_environment_source, zinder_environment_source_from_map,
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
pub use logging::{
    RuntimeTracing, TracingSubscriberError, install_runtime_tracing_subscriber,
    install_tracing_subscriber, trace_ingest_control, trace_node_rpc,
};
pub use memory_budget::{
    CgroupMemorySnapshot, canonical_reader_block_cache_bytes, canonical_reader_max_open_files,
    container_memory_budget_bytes, container_memory_snapshot,
    wallet_build_outpoint_sort_memory_bytes, wallet_build_outpoint_sort_memory_bytes_for,
    wallet_build_reorg_undo_memory_bytes, wallet_build_reorg_undo_memory_bytes_for,
    wallet_build_secondary_sort_memory_bytes_per_sorter,
    wallet_build_secondary_sort_memory_bytes_per_sorter_for,
};
pub use metrics::{
    MetricsHandle, MetricsInstallError, install_metrics_recorder,
    install_metrics_recorder_for_service,
};
pub use ops_endpoint::{
    OpsEndpointHandle, OpsServer, OpsServerError, OpsStatus, ServedChainStatus, serve_ops_endpoint,
    spawn_ops_endpoint, spawn_ops_endpoint_for,
};
pub use ops_healthcheck::{
    DEFAULT_OPS_HEALTHCHECK_PATH, OPS_HEALTHCHECK_TIMEOUT, OPS_LISTEN_ADDR_ENV_VAR,
    OpsHealthcheckArgs, OpsHealthcheckError, config_file_ops_listen_addr, probe_ops_endpoint,
    resolve_ops_healthcheck_addr,
};
pub use process::ProcessMemory;
pub use public_grpc_server::{
    ClientAddressSource, DEFAULT_GRPC_RECEIVE_SLOTS, DEFAULT_GRPC_RECEIVE_TIMEOUT,
    DEFAULT_GRPC_SEND_STALL_TIMEOUT, GrpcCallTermination, GrpcCallTerminationFuture,
    GrpcCallTerminationLayer, GrpcDeadline, GrpcProxyProtocol, GrpcRangeAdmission,
    GrpcReceiveLimit, GrpcReceiveLimitLayer, GrpcReceiveLimits, GrpcSendStall, GrpcSendStallLayer,
    HTTP2_MAX_LOCAL_ERROR_RESETS, MAX_CONCURRENT_STREAMS_PER_CONNECTION,
    PROXY_CLIENT_ADDRESS_HEADER, PublicGrpcLayers, PublicGrpcRouter, PublicGrpcServer,
    RangeClientKey, RetryPushback, WalletRpcMetrics, WalletRpcMetricsLayer, public_grpc_reflection,
    public_grpc_server, public_grpc_server_with,
};
pub use readiness::{
    ActiveNodeEndpoint, FollowWaitReason, IngestPhase, NodeUnavailableDetail, Readiness,
    ReadinessCause, ReadinessDependency, ReadinessReport, ReadinessState,
    TrafficReadinessInterceptor, UpstreamHealth, UpstreamNotReadyDetail, not_ready_status,
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
    DeploymentProfile, DeploymentSection, DeploymentToml, IngestControlReaderToml,
    IngestControlSection, IngestControlWriterToml, ObservabilityConfig, OpsSection, OpsToml,
    ProjectorControlSection, ProjectorControlToml, ResolvedCanonicalSecondaryStorage,
    ResolvedIngestControlReader, ResolvedIngestControlWriter, ResolvedProjectorControl,
    ResolvedRetention, ResolvedSecondaryStorage, RetentionSection, RetentionToml,
    RocksDbResourceBudgetSection, RocksDbResourceBudgetToml, RuntimeService,
    SecondaryStorageSection, SecondaryStorageToml, SecuritySection, SecurityToml,
    StorageRoleSection, StorageRoleToml, defaults as section_defaults, derive_canonical_path,
    derive_canonical_secondary_path, derive_checkpoints_path, derive_explorer_secondary_path,
    derive_wallet_path, derive_wallet_secondary_path, resolve_allow_public_bind,
    resolve_canonical_reader_rocksdb_budget, resolve_canonical_secondary_storage,
    resolve_canonical_writer_rocksdb_budget, resolve_ingest_control_reader,
    resolve_ingest_control_writer, resolve_ingest_control_writer_with_data_dir,
    resolve_manual_raw_blob_retention, resolve_materialized_view_reader_rocksdb_budget,
    resolve_materialized_view_writer_rocksdb_budget, resolve_ops_listen_addr,
    resolve_projector_control, resolve_projector_control_with_data_dir, resolve_retention,
    resolve_secondary_storage, resolve_wallet_projection_reader_rocksdb_budget,
    resolve_wallet_projection_writer_rocksdb_budget, validate_optional_data_dir,
};
pub use startup_phase::{StartupPhase, StartupPhaseGuard};
pub use transport::{
    DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT, InvalidZinderGrpcEndpoint, LazyZinderGrpcChannel,
    connect_zinder_grpc, connect_zinder_grpc_at_startup, connect_zinder_grpc_with_timeout,
    validate_zinder_grpc_endpoint,
};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Upper bound on the size of an incoming gRPC request message, applied to
/// every serving Zinder gRPC server via `max_decoding_message_size`.
///
/// The same cap applies to the decompressed payload and zstd window on
/// public application services.
///
/// The largest legitimate request is a broadcast transaction of
/// [`zinder_core::MAX_RAW_TRANSACTION_BYTES`] (2,000,000 bytes), which
/// encodes to at most 2,000,015 bytes as a lightwalletd `RawTransaction` and
/// fits under 2 MiB with room to spare. A larger transaction that still fits
/// is rejected by the broadcast guard with its typed error. The transport
/// rejects an oversized frame with `OUT_OF_RANGE` and an oversized
/// decompressed payload with `RESOURCE_EXHAUSTED`.
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
/// unconditionally. The startup check reports missing features before opening
/// storage. Deployments must meet the baseline even for code that runs before
/// this check.
#[must_use]
pub fn host_cpu_meets_compiled_baseline() -> bool {
    #[cfg(target_arch = "x86_64")]
    if cfg!(target_feature = "avx2") && !host_has_avx_state() {
        tracing::error!(
            target: "zinder::runtime",
            event = "host_cpu_below_compiled_baseline",
            missing = "avx",
            "AVX requires OS-enabled XMM and YMM state"
        );
        return false;
    }
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
    let cpu = raw_cpuid::CpuId::new();
    let basic = cpu.get_feature_info();
    let extended = host_extended_features();
    let required = [
        (
            "avx2",
            extended
                .as_ref()
                .is_some_and(raw_cpuid::ExtendedFeatures::has_avx2),
        ),
        (
            "bmi1",
            extended
                .as_ref()
                .is_some_and(raw_cpuid::ExtendedFeatures::has_bmi1),
        ),
        (
            "bmi2",
            extended
                .as_ref()
                .is_some_and(raw_cpuid::ExtendedFeatures::has_bmi2),
        ),
        (
            "fma",
            basic.as_ref().is_some_and(raw_cpuid::FeatureInfo::has_fma),
        ),
        (
            "lzcnt",
            cpu.get_extended_processor_and_feature_identifiers()
                .is_some_and(|features| features.has_lzcnt()),
        ),
        (
            "pclmulqdq",
            basic
                .as_ref()
                .is_some_and(raw_cpuid::FeatureInfo::has_pclmulqdq),
        ),
    ];
    required
        .into_iter()
        .filter(|(_, detected)| !detected)
        .map(|(feature, _)| feature)
        .collect()
}

#[cfg(target_arch = "x86_64")]
fn host_has_avx_state() -> bool {
    raw_cpuid::CpuId::new()
        .get_feature_info()
        .is_some_and(|features| {
            host_avx_enabled(
                features.has_avx() && features.has_xsave() && features.has_oxsave(),
                x86_64::registers::xcontrol::XCr0::read,
            )
        })
}

#[cfg(target_arch = "x86_64")]
fn host_avx_enabled(
    can_read_avx_state: bool,
    read_xcr0: impl FnOnce() -> x86_64::registers::xcontrol::XCr0Flags,
) -> bool {
    use x86_64::registers::xcontrol::XCr0Flags;

    // XGETBV is valid only after the CPU and OS have enabled XSAVE.
    can_read_avx_state && read_xcr0().contains(XCr0Flags::SSE | XCr0Flags::AVX)
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
fn missing_required_cpu_features() -> Vec<&'static str> {
    Vec::new()
}

/// The Pasta field-arithmetic backend compiled into this build, from
/// `pasta_curves`: `"portable"`, `"x86-64"`, `"aarch64"`, or `"armv7em"`.
///
/// `pasta_curves` selects the backend at compile time from its `asm` feature
/// and the target. Release images, binary archives, and Compose builds enable
/// `pasta_curves/asm` by default (`ZINDER_PASTA_ASM=1`; set `0` for portable);
/// logging this backend at startup makes any accidental portable fallback
/// immediately visible.
#[must_use]
pub const fn pasta_backend() -> &'static str {
    pasta_curves::BACKEND
}

/// Verifies the host CPU provides every instruction-set feature the compiled
/// Pasta backend requires.
///
/// Logs the selected backend and returns `false` when the host falls short.
/// The `pasta_curves` x86-64 assembly backend (enabled by default in release
/// images and binary archives via `ZINDER_PASTA_ASM=1`) uses BMI2 and ADX
/// instructions without runtime dispatch. Because ADX lies above the base
/// `x86-64-v3` profile, the runtime fails closed before storage opens rather
/// than crashing with `SIGILL` on pre-Broadwell/Haswell CPUs (rebuild with
/// `ZINDER_PASTA_ASM=0` for such hosts). The portable backend and the `AArch64`
/// and `ARMv7E-M` backends require nothing beyond their target. This is a
/// separate check from [`host_cpu_meets_compiled_baseline`], which covers the
/// base `x86-64-v3` features every release binary requires.
#[must_use]
pub fn host_cpu_meets_pasta_backend_baseline() -> bool {
    tracing::info!(
        target: "zinder::runtime",
        event = "pasta_backend_selected",
        backend = pasta_backend(),
        "Pasta field-arithmetic backend compiled into this build"
    );
    let missing = missing_pasta_backend_cpu_features();
    if missing.is_empty() {
        return true;
    }
    tracing::error!(
        target: "zinder::runtime",
        event = "host_cpu_below_pasta_backend_baseline",
        backend = pasta_backend(),
        missing = missing.join(", "),
        "this build compiled the Pasta x86-64 assembly backend, which requires these CPU features"
    );
    false
}

#[cfg(any(target_arch = "x86_64", test))]
fn missing_pasta_features(backend: &str, bmi2: bool, adx: bool) -> Vec<&'static str> {
    let mut missing = Vec::new();
    if backend == "x86-64" {
        if !bmi2 {
            missing.push("bmi2");
        }
        if !adx {
            missing.push("adx");
        }
    }
    missing
}

#[cfg(target_arch = "x86_64")]
fn host_extended_features() -> Option<raw_cpuid::ExtendedFeatures> {
    // Rust's detection macro folds features enabled by target-cpu to true.
    raw_cpuid::CpuId::new().get_extended_feature_info()
}

#[cfg(target_arch = "x86_64")]
fn missing_pasta_backend_cpu_features() -> Vec<&'static str> {
    let features = host_extended_features();
    missing_pasta_features(
        pasta_backend(),
        features
            .as_ref()
            .is_some_and(raw_cpuid::ExtendedFeatures::has_bmi2),
        features
            .as_ref()
            .is_some_and(raw_cpuid::ExtendedFeatures::has_adx),
    )
}

#[cfg(not(target_arch = "x86_64"))]
fn missing_pasta_backend_cpu_features() -> Vec<&'static str> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::{missing_pasta_features, pasta_backend};

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn avx_requires_os_enabled_xmm_and_ymm_state() {
        use x86_64::registers::xcontrol::XCr0Flags;

        let mut xcr0_read = false;
        assert!(!super::host_avx_enabled(false, || {
            xcr0_read = true;
            XCr0Flags::all()
        }));
        assert!(!xcr0_read);
        for state in [XCr0Flags::empty(), XCr0Flags::SSE, XCr0Flags::AVX] {
            assert!(!super::host_avx_enabled(true, || state));
        }
        assert!(super::host_avx_enabled(true, || XCr0Flags::SSE | XCr0Flags::AVX));
    }

    #[test]
    fn pasta_backend_names_a_known_backend() {
        assert!(
            ["portable", "x86-64", "aarch64", "armv7em"].contains(&pasta_backend()),
            "unexpected Pasta backend: {}",
            pasta_backend()
        );
    }

    #[test]
    fn pasta_guard_requires_both_x86_assembly_features() {
        assert_eq!(
            missing_pasta_features("x86-64", false, false),
            ["bmi2", "adx"]
        );
        assert_eq!(missing_pasta_features("x86-64", true, false), ["adx"]);
        assert_eq!(missing_pasta_features("x86-64", false, true), ["bmi2"]);
        assert_eq!(
            missing_pasta_features("x86-64", true, true),
            [] as [&str; 0]
        );
        for backend in ["portable", "aarch64", "armv7em"] {
            assert_eq!(
                missing_pasta_features(backend, false, false),
                [] as [&str; 0]
            );
        }
    }
}
