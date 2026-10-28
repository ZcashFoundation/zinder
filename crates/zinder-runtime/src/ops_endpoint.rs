//! Operational HTTP endpoints (`/healthz`, `/readyz`, `/metrics`) shared by services.

use std::{net::SocketAddr, sync::Arc};

use axum::{Json, Router, http::StatusCode, routing::get};
use serde::Serialize;
use thiserror::Error;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    BUILD_GIT_COMMIT, MetricsHandle, MetricsInstallError, Readiness, install_metrics_recorder,
    sections::RuntimeService,
};

/// Service identity surfaced by `/metrics` for build-time labeling.
#[derive(Clone, Debug)]
pub struct OpsServer {
    /// Service name (`zinder-ingest`, `zinder-query`, ...).
    pub service_name: &'static str,
    /// Service version, typically `env!("CARGO_PKG_VERSION")`.
    pub service_version: &'static str,
    /// Network this binary is operating on.
    pub network_name: &'static str,
    /// Immutable capability snapshot shared with the serving boundary.
    ///
    /// Surfaced verbatim through the `/healthz` JSON for discoverability so
    /// dashboards and `curl` probes can branch without a gRPC round trip.
    /// The snapshot is fixed at startup: late-binding capabilities
    /// (those gated on a runtime upstream probe) appear here only when the
    /// binary computed them before calling `spawn_ops_endpoint*`. Each
    /// capability string must come from
    /// `zinder_proto::capabilities`.
    pub advertised_capabilities: Arc<[&'static str]>,
}

/// Error returned by the operational HTTP server.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OpsServerError {
    /// Failed to bind the operational listen address.
    #[error("failed to bind operational endpoint at {listen_addr}: {source}")]
    Bind {
        /// Address that failed to bind.
        listen_addr: SocketAddr,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Underlying axum/hyper transport failed.
    #[error("operational endpoint server failed: {source}")]
    Transport {
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Failed to install the metrics recorder used by `/metrics`.
    #[error(transparent)]
    Metrics(#[from] MetricsInstallError),

    /// The spawned operational endpoint task could not be joined.
    #[error("operational endpoint task failed: {source}")]
    TaskJoin {
        /// Underlying task failure.
        #[source]
        source: tokio::task::JoinError,
    },
}

/// Handle for a spawned operational HTTP server.
///
/// Use [`OpsEndpointHandle::wait`] to supervise unexpected exit or
/// [`OpsEndpointHandle::shutdown`] for graceful shutdown.
/// Dropping the handle requests cancellation so later startup failures cannot
/// orphan the endpoint, but only `wait` or `shutdown` can report task errors.
pub struct OpsEndpointHandle {
    cancel: CancellationToken,
    join: Option<JoinHandle<Result<(), OpsServerError>>>,
}

impl OpsEndpointHandle {
    /// Waits for the operational server task to exit.
    ///
    /// This method is cancellation-safe. A caller may select on it alongside
    /// another server future and still call [`Self::shutdown`] if the other
    /// future completes first.
    pub async fn wait(&mut self) -> Result<(), OpsServerError> {
        let Some(join) = self.join.as_mut() else {
            return Ok(());
        };
        let joined = join.await;
        self.join = None;
        joined.map_err(|source| OpsServerError::TaskJoin { source })?
    }

    /// Cancels the operational server and awaits its task to completion.
    pub async fn shutdown(mut self) -> Result<(), OpsServerError> {
        self.cancel.cancel();
        self.wait().await
    }
}

impl Drop for OpsEndpointHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Spawns the operational HTTP server for a known runtime service when
/// `listen_addr` is populated.
///
/// Returns `Ok(None)` when the operator opted out (empty string in
/// `ops.listen_addr` resolves to `None` before this function is called).
/// Otherwise binds the endpoint and returns the spawned handle, identical to
/// [`spawn_ops_endpoint`] but with `service_name` filled in from the
/// [`RuntimeService`] table so each binary cannot drift its own label.
#[must_use = "drop the returned handle only on graceful shutdown"]
#[allow(
    clippy::too_many_arguments,
    reason = "runtime service, listen address, version, network, readiness, and capability snapshot are all binding-time inputs; bundling them into a struct would only push the field count one layer out"
)]
pub async fn spawn_ops_endpoint_for(
    service: RuntimeService,
    listen_addr: Option<SocketAddr>,
    service_version: &'static str,
    network_name: &'static str,
    readiness: Readiness,
    advertised_capabilities: Arc<[&'static str]>,
) -> Result<Option<OpsEndpointHandle>, OpsServerError> {
    let Some(listen_addr) = listen_addr else {
        return Ok(None);
    };
    spawn_ops_endpoint(
        listen_addr,
        OpsServer {
            service_name: service.binary_name(),
            service_version,
            network_name,
            advertised_capabilities,
        },
        readiness,
    )
    .await
    .map(Some)
}

/// Binds and spawns the operational HTTP server on a tokio task.
///
/// The returned handle proves that the listen address was reserved and the
/// process-wide metrics recorder was installed successfully. Use this from
/// binary `main` paths; for tests that need direct access to the future, call
/// [`serve_ops_endpoint`] instead.
#[must_use = "the returned handle owns the spawned task; drop only on graceful shutdown"]
pub async fn spawn_ops_endpoint(
    listen_addr: SocketAddr,
    server: OpsServer,
    readiness: Readiness,
) -> Result<OpsEndpointHandle, OpsServerError> {
    let listener = bind_ops_listener(listen_addr).await?;
    let metrics = install_metrics_recorder(&server)?;
    let cancel = CancellationToken::new();
    let cancel_for_task = cancel.clone();
    let join = tokio::spawn(async move {
        serve_ops_endpoint_on_listener(listener, server, readiness, cancel_for_task, metrics).await
    });

    Ok(OpsEndpointHandle {
        cancel,
        join: Some(join),
    })
}

/// Serves `/healthz`, `/readyz`, and `/metrics` until `cancel` fires.
///
/// `readiness` is shared with the runtime so updates are visible to HTTP
/// handlers without copying.
pub async fn serve_ops_endpoint(
    listen_addr: SocketAddr,
    server: OpsServer,
    readiness: Readiness,
    cancel: CancellationToken,
) -> Result<(), OpsServerError> {
    let listener = bind_ops_listener(listen_addr).await?;
    let metrics = install_metrics_recorder(&server)?;
    serve_ops_endpoint_on_listener(listener, server, readiness, cancel, metrics).await
}

async fn bind_ops_listener(listen_addr: SocketAddr) -> Result<TcpListener, OpsServerError> {
    TcpListener::bind(listen_addr)
        .await
        .map_err(|source| OpsServerError::Bind {
            listen_addr,
            source,
        })
}

async fn serve_ops_endpoint_on_listener(
    listener: TcpListener,
    server: OpsServer,
    readiness: Readiness,
    cancel: CancellationToken,
    metrics: MetricsHandle,
) -> Result<(), OpsServerError> {
    let listen_addr = listener.local_addr().map_or_else(
        |error| format!("unknown ({error})"),
        |listen_addr| listen_addr.to_string(),
    );
    let app = build_router(&server, readiness, metrics);

    tracing::info!(
        target: "zinder::runtime",
        event = "ops_endpoint_started",
        listen_addr = %listen_addr,
        "operational endpoint started"
    );

    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(cancel.cancelled_owned())
        .await;

    tracing::info!(
        target: "zinder::runtime",
        event = "ops_endpoint_stopped",
        "operational endpoint stopped"
    );

    serve_result.map_err(|source| OpsServerError::Transport { source })
}

fn build_router(server: &OpsServer, readiness: Readiness, metrics: MetricsHandle) -> Router {
    let metrics_state = MetricsState {
        service_name: server.service_name,
        service_version: server.service_version,
        build_git_commit: BUILD_GIT_COMMIT,
        network_name: server.network_name,
        readiness: readiness.clone(),
        metrics,
    };
    let healthz_body = HealthzBody {
        status: "alive",
        service: server.service_name,
        version: server.service_version,
        git_commit: BUILD_GIT_COMMIT,
        network: server.network_name,
        capabilities: server
            .advertised_capabilities
            .iter()
            .map(|capability| (*capability).to_owned())
            .collect(),
    };

    Router::new()
        .route(
            "/healthz",
            get(move || {
                let body = healthz_body.clone();
                async move { (StatusCode::OK, Json(body)) }
            }),
        )
        .route(
            "/readyz",
            get(move || {
                let readiness = readiness.clone();
                async move { readyz_handler(&readiness) }
            }),
        )
        .route(
            "/metrics",
            get(move || {
                let metrics_state = metrics_state.clone();
                async move { metrics_handler(&metrics_state) }
            }),
        )
}

#[derive(Clone)]
struct MetricsState {
    service_name: &'static str,
    service_version: &'static str,
    build_git_commit: &'static str,
    network_name: &'static str,
    readiness: Readiness,
    metrics: MetricsHandle,
}

#[derive(Clone, Serialize)]
struct HealthzBody {
    status: &'static str,
    service: &'static str,
    version: &'static str,
    git_commit: &'static str,
    network: &'static str,
    capabilities: Vec<String>,
}

#[derive(Serialize)]
struct ReadinessResponseBody {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    phase: Option<crate::IngestPhase>,
    cause: crate::ReadinessCause,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    materialized_view_preset: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    materialized_view_identities: Vec<String>,
}

fn readyz_handler(readiness: &Readiness) -> (StatusCode, Json<ReadinessResponseBody>) {
    let report = readiness.report();
    let status_code = if report.is_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let body = ReadinessResponseBody {
        status: if report.is_ready {
            "ready"
        } else {
            "not_ready"
        },
        phase: report.phase,
        cause: report.cause,
        current_height: report.current_height,
        target_height: report.target_height,
        materialized_view_preset: report.materialized_view_preset,
        materialized_view_identities: report.materialized_view_identities,
    };

    (status_code, Json(body))
}

fn metrics_handler(state: &MetricsState) -> (StatusCode, String) {
    metrics::gauge!(
        "zinder_build_info",
        "service" => state.service_name,
        "version" => state.service_version,
        "git_commit" => state.build_git_commit,
        "network" => state.network_name
    )
    .set(1.0);
    record_readiness_metrics(state);
    (StatusCode::OK, state.metrics.render())
}

fn record_readiness_metrics(state: &MetricsState) {
    let report = state.readiness.report();
    if let Some(materialized_view_preset) = &report.materialized_view_preset {
        metrics::gauge!(
            "zinder_materialized_view_workload_info",
            "service" => state.service_name,
            "network" => state.network_name,
            "preset" => materialized_view_preset.clone(),
        )
        .set(1.0);
        for materialized_view_identity in &report.materialized_view_identities {
            metrics::gauge!(
                "zinder_materialized_view_identity_info",
                "service" => state.service_name,
                "network" => state.network_name,
                "preset" => materialized_view_preset.clone(),
                "identity" => materialized_view_identity.clone(),
            )
            .set(1.0);
        }
    }
    let active_cause = report.cause.metric_label();
    for cause in crate::ReadinessCause::ALL_METRIC_LABELS {
        metrics::gauge!(
            "zinder_readiness_state",
            "service" => state.service_name,
            "network" => state.network_name,
            "cause" => *cause
        )
        .set(if *cause == active_cause { 1.0 } else { 0.0 });
    }

    let active_failure_class = report.cause.node_failure_class_label();
    for class_label in zinder_source::SourceFailureClass::ALL_LABELS {
        metrics::gauge!(
            "zinder_readiness_node_failure_class",
            "service" => state.service_name,
            "network" => state.network_name,
            "class" => *class_label
        )
        .set(if Some(*class_label) == active_failure_class {
            1.0
        } else {
            0.0
        });
    }

    metrics::gauge!(
        "zinder_readiness_sync_lag_blocks",
        "service" => state.service_name,
        "network" => state.network_name
    )
    .set(readiness_sync_lag_blocks(&report.cause));

    metrics::gauge!(
        "zinder_readiness_replica_lag_chain_epochs",
        "service" => state.service_name,
        "network" => state.network_name
    )
    .set(readiness_replica_lag_chain_epochs(&report.cause));
}

fn readiness_sync_lag_blocks(cause: &crate::ReadinessCause) -> f64 {
    match cause {
        crate::ReadinessCause::Syncing {
            lag_blocks: Some(lag_blocks),
        } => u64_to_f64(*lag_blocks),
        crate::ReadinessCause::Starting
        | crate::ReadinessCause::Syncing { lag_blocks: None }
        | crate::ReadinessCause::Ready
        | crate::ReadinessCause::NodeUnavailable(_)
        | crate::ReadinessCause::NodeCapabilityMissing { .. }
        | crate::ReadinessCause::StorageUnavailable
        | crate::ReadinessCause::SchemaMismatch
        | crate::ReadinessCause::ReorgWindowExceeded { .. }
        | crate::ReadinessCause::ReplicaLagging { .. }
        | crate::ReadinessCause::ServingPairStale { .. }
        | crate::ReadinessCause::WriterStatusUnavailable
        | crate::ReadinessCause::IngestControlUnavailable
        | crate::ReadinessCause::CanonicalTipUnconfirmed
        | crate::ReadinessCause::CursorAtRisk { .. }
        | crate::ReadinessCause::ShuttingDown
        | crate::ReadinessCause::UpstreamNotReady(_) => 0.0,
    }
}

fn readiness_replica_lag_chain_epochs(cause: &crate::ReadinessCause) -> f64 {
    match cause {
        crate::ReadinessCause::ReplicaLagging { lag_chain_epochs }
        | crate::ReadinessCause::ServingPairStale {
            lag_chain_epochs, ..
        } => u64_to_f64(*lag_chain_epochs),
        crate::ReadinessCause::Starting
        | crate::ReadinessCause::Syncing { .. }
        | crate::ReadinessCause::Ready
        | crate::ReadinessCause::NodeUnavailable(_)
        | crate::ReadinessCause::NodeCapabilityMissing { .. }
        | crate::ReadinessCause::StorageUnavailable
        | crate::ReadinessCause::SchemaMismatch
        | crate::ReadinessCause::ReorgWindowExceeded { .. }
        | crate::ReadinessCause::WriterStatusUnavailable
        | crate::ReadinessCause::IngestControlUnavailable
        | crate::ReadinessCause::CanonicalTipUnconfirmed
        | crate::ReadinessCause::CursorAtRisk { .. }
        | crate::ReadinessCause::ShuttingDown
        | crate::ReadinessCause::UpstreamNotReady(_) => 0.0,
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus gauges use f64 samples; readiness lag is diagnostic"
)]
fn u64_to_f64(sample: u64) -> f64 {
    sample as f64
}

#[cfg(test)]
mod tests {
    use super::{OpsEndpointHandle, OpsServerError};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn wait_surfaces_unexpected_server_exit() -> Result<(), Box<dyn std::error::Error>> {
        let join = tokio::spawn(async {
            Err(OpsServerError::Transport {
                source: std::io::Error::other("synthetic server failure"),
            })
        });
        let mut handle = OpsEndpointHandle {
            cancel: CancellationToken::new(),
            join: Some(join),
        };

        let error = handle
            .wait()
            .await
            .err()
            .ok_or("unexpected server exit must be observable")?;

        assert!(matches!(error, OpsServerError::Transport { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn wait_surfaces_task_join_failure_once() -> Result<(), Box<dyn std::error::Error>> {
        let join = tokio::spawn(std::future::pending::<Result<(), OpsServerError>>());
        join.abort();
        let mut handle = OpsEndpointHandle {
            cancel: CancellationToken::new(),
            join: Some(join),
        };

        let error = handle
            .wait()
            .await
            .err()
            .ok_or("task join failure must be observable")?;

        assert!(matches!(
            error,
            OpsServerError::TaskJoin { source } if source.is_cancelled()
        ));
        assert!(handle.wait().await.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn dropping_handle_requests_server_cancellation() -> Result<(), Box<dyn std::error::Error>>
    {
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            cancel_for_task.cancelled().await;
            let _ = stopped_tx.send(());
            Ok(())
        });
        let handle = OpsEndpointHandle {
            cancel,
            join: Some(join),
        };

        drop(handle);

        tokio::time::timeout(std::time::Duration::from_secs(1), stopped_rx).await??;
        Ok(())
    }
}
