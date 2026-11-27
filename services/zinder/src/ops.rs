//! T=0 operational HTTP server (`/healthz`, `/readyz`, `/metrics`) for `zinder`.

use std::{collections::HashSet, fmt::Write as _, net::SocketAddr, sync::Arc, time::Duration};

use axum::{Json, Router, body::Bytes, extract::State, http::StatusCode, routing::get};
use http_body_util::{BodyExt, Empty};
use hyper::Request;
use hyper_util::rt::TokioIo;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use zinder_runtime::{
    BUILD_GIT_COMMIT, HttpListenerLimits, MetricsHandle, MetricsInstallError, OpsServer,
    install_metrics_recorder, serve_http,
};

use crate::cli::ServiceRole;

const OPS_MAX_REQUEST_BODY_BYTES: usize = 4 * 1024;
const OPS_MAX_CONNECTIONS: usize = 64;
const CHILD_SCRAPE_TIMEOUT: Duration = Duration::from_millis(750);

/// High-level startup and readiness stage reported by the supervisor `/readyz` endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupStage {
    /// Initializing canonical primary storage and catching up tip ingestion.
    BootstrappingCanonical,
    /// Catching up wallet projection from the canonical store.
    CatchingUpWalletProjection,
    /// Waiting for Explorer materialized views and `ExplorerQuery` readiness.
    WaitingForExplorerViews,
    /// Waiting for public serving listeners to transition ready.
    WaitingForServingListeners,
    /// All required roles are healthy and serving.
    Ready,
    /// Draining supervised child processes on shutdown.
    ShuttingDown,
    /// A supervised role failed unexpectedly.
    Failed,
}

impl StartupStage {
    /// Returns the `snake_case` wire string for this stage.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BootstrappingCanonical => "bootstrapping_canonical",
            Self::CatchingUpWalletProjection => "catching_up_wallet_projection",
            Self::WaitingForExplorerViews => "waiting_for_explorer_views",
            Self::WaitingForServingListeners => "waiting_for_serving_listeners",
            Self::Ready => "ready",
            Self::ShuttingDown => "shutting_down",
            Self::Failed => "failed",
        }
    }
}

/// Live snapshot of one supervised child role tracked by the operational server.
#[derive(Clone, Debug, Serialize)]
pub struct RoleOpsSnapshot {
    /// Role name (`ingest`, `projector`, `query`, etc.).
    pub role: &'static str,
    /// Whether this role must report ready before `/readyz` returns HTTP 200.
    pub required: bool,
    /// Current lifecycle state (`starting`, `ready`, `not_ready`, `exited`, `stopped`).
    pub state: String,
    /// Whether `/readyz` on this role returned HTTP 200 on the latest poll.
    pub ready: bool,
    /// Optional readiness cause string reported by the child role.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
    /// Optional startup phase reported by the child role.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Current chain height reported by the child role.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_height: Option<u32>,
    /// Target chain height reported by the child role.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_height: Option<u32>,
    /// Diagnostic detail when exited or unavailable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip)]
    pub(crate) ops_addr: Option<SocketAddr>,
    #[serde(skip)]
    pub(crate) role_kind: ServiceRole,
}

impl RoleOpsSnapshot {
    /// Creates an initial `starting` snapshot for a role.
    #[must_use]
    pub fn starting(role: ServiceRole, required: bool, ops_addr: Option<SocketAddr>) -> Self {
        Self {
            role: role.label(),
            required,
            state: "starting".to_owned(),
            ready: false,
            cause: None,
            phase: None,
            current_height: None,
            target_height: None,
            detail: None,
            ops_addr,
            role_kind: role,
        }
    }
}

/// Shared operational state updated by the supervisor and served over HTTP.
#[derive(Clone, Debug)]
pub struct SharedOpsState {
    inner: Arc<RwLock<OpsStateInner>>,
}

#[derive(Debug)]
struct OpsStateInner {
    mode: &'static str,
    preset: &'static str,
    network_name: &'static str,
    stage: StartupStage,
    roles: Vec<RoleOpsSnapshot>,
}

impl SharedOpsState {
    /// Constructs initial operational state prior to launching child roles.
    #[must_use]
    pub fn new(
        mode: &'static str,
        preset: &'static str,
        network_name: &'static str,
        roles: Vec<RoleOpsSnapshot>,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(OpsStateInner {
                mode,
                preset,
                network_name,
                stage: StartupStage::BootstrappingCanonical,
                roles,
            })),
        }
    }

    /// Replaces the tracked startup stage and role snapshots.
    pub fn update(&self, stage: StartupStage, roles: Vec<RoleOpsSnapshot>) {
        let mut guard = self.inner.write();
        guard.stage = stage;
        guard.roles = roles;
    }

    /// Updates only the startup stage while preserving existing role snapshots.
    pub fn set_stage(&self, stage: StartupStage) {
        self.inner.write().stage = stage;
    }

    /// Snapshots all child internal operational addresses currently alive.
    #[must_use]
    pub fn scrape_targets(&self) -> Vec<(ServiceRole, SocketAddr)> {
        let guard = self.inner.read();
        guard
            .roles
            .iter()
            .filter_map(|snapshot| {
                (snapshot.state != "exited" && snapshot.state != "stopped")
                    .then_some(snapshot.ops_addr.map(|addr| (snapshot.role_kind, addr)))
                    .flatten()
            })
            .collect()
    }

    fn network_name(&self) -> &'static str {
        self.inner.read().network_name
    }

    fn mode(&self) -> &'static str {
        self.inner.read().mode
    }

    fn preset(&self) -> &'static str {
        self.inner.read().preset
    }

    fn ready_summary(&self) -> ReadyzResponseBody {
        let guard = self.inner.read();
        let ready = guard.stage == StartupStage::Ready
            && !guard.roles.is_empty()
            && guard
                .roles
                .iter()
                .all(|snapshot| !snapshot.required || snapshot.ready);
        ReadyzResponseBody {
            status: if ready { "ready" } else { "not_ready" },
            mode: guard.mode,
            preset: guard.preset,
            network: guard.network_name,
            phase: guard.stage,
            roles: guard.roles.clone(),
        }
    }
}

/// Errors returned while binding or serving the supervisor operational endpoint.
#[derive(Debug, Error)]
pub enum SupervisorOpsError {
    /// Failed to bind the operational socket address.
    #[error("failed to bind operational endpoint at {listen_addr}: {source}")]
    Bind {
        /// Requested bind address.
        listen_addr: SocketAddr,
        /// Underlying OS bind error.
        #[source]
        source: std::io::Error,
    },
    /// Failed to install the process-wide metrics recorder.
    #[error(transparent)]
    Metrics(#[from] MetricsInstallError),
    /// HTTP listener serving failed.
    #[error("operational endpoint HTTP server failed: {source}")]
    Transport {
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

/// Owned handle to the running T=0 supervisor operational HTTP server.
pub struct SupervisorOpsHandle {
    cancel: CancellationToken,
    join: Option<JoinHandle<Result<(), SupervisorOpsError>>>,
}

impl SupervisorOpsHandle {
    /// Requests shutdown of the operational HTTP server and awaits exit.
    pub async fn shutdown(mut self) -> Result<(), SupervisorOpsError> {
        self.cancel.cancel();
        if let Some(join) = self.join.take()
            && let Ok(outcome) = join.await
        {
            return outcome;
        }
        Ok(())
    }
}

impl Drop for SupervisorOpsHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[derive(Clone, Serialize)]
struct HealthzBody {
    status: &'static str,
    service: &'static str,
    mode: &'static str,
    preset: &'static str,
    version: &'static str,
    git_commit: &'static str,
    network: &'static str,
}

#[derive(Serialize)]
struct ReadyzResponseBody {
    status: &'static str,
    mode: &'static str,
    preset: &'static str,
    network: &'static str,
    phase: StartupStage,
    roles: Vec<RoleOpsSnapshot>,
}

#[derive(Clone)]
struct SupervisorHttpState {
    healthz: HealthzBody,
    shared: SharedOpsState,
    metrics: MetricsHandle,
}

/// Child `/readyz` response payload fields extracted by supervisor polling.
#[derive(Debug, Default, Deserialize)]
pub struct ChildReadyzPayload {
    /// Child readiness string (`ready` / `not_ready`).
    pub status: Option<String>,
    /// Child readiness cause label.
    pub cause: Option<String>,
    /// Child startup phase label.
    pub phase: Option<String>,
    /// Current synced chain height.
    pub current_height: Option<u32>,
    /// Target upstream height.
    pub target_height: Option<u32>,
}

/// Binds the operational HTTP listener immediately at T=0 and serves `/healthz`,
/// `/readyz`, and `/metrics` in the background.
pub async fn spawn_supervisor_ops(
    listen_addr: SocketAddr,
    shared: SharedOpsState,
) -> Result<SupervisorOpsHandle, SupervisorOpsError> {
    let listener =
        TcpListener::bind(listen_addr)
            .await
            .map_err(|source| SupervisorOpsError::Bind {
                listen_addr,
                source,
            })?;
    let server_descriptor = OpsServer {
        service_name: "zinder",
        service_version: env!("CARGO_PKG_VERSION"),
        network_name: shared.network_name(),
        advertised_capabilities: Arc::from([]),
    };
    let metrics = install_metrics_recorder(&server_descriptor)?;
    let healthz = HealthzBody {
        status: "alive",
        service: "zinder",
        mode: shared.mode(),
        preset: shared.preset(),
        version: env!("CARGO_PKG_VERSION"),
        git_commit: BUILD_GIT_COMMIT,
        network: shared.network_name(),
    };
    let state = SupervisorHttpState {
        healthz,
        shared,
        metrics,
    };
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let join = tokio::spawn(async move { serve_loop(listener, state, task_cancel).await });
    Ok(SupervisorOpsHandle {
        cancel,
        join: Some(join),
    })
}

async fn serve_loop(
    listener: TcpListener,
    state: SupervisorHttpState,
    cancel: CancellationToken,
) -> Result<(), SupervisorOpsError> {
    let bound_addr = listener.local_addr().map_or_else(
        |error| format!("unknown ({error})"),
        |addr| addr.to_string(),
    );
    let app = Router::new()
        .route("/healthz", get(handle_healthz))
        .route("/readyz", get(handle_readyz))
        .route("/metrics", get(handle_metrics))
        .with_state(state);

    tracing::info!(
        target: "zinder::supervisor",
        event = "supervisor_ops_started",
        listen_addr = %bound_addr,
        "supervisor operational endpoint listening"
    );
    let limits = HttpListenerLimits {
        max_request_body_bytes: OPS_MAX_REQUEST_BODY_BYTES,
        max_connections: OPS_MAX_CONNECTIONS,
        ..HttpListenerLimits::default()
    };
    let outcome = serve_http(listener, app, limits, cancel.cancelled_owned()).await;
    outcome.map_err(|source| SupervisorOpsError::Transport { source })
}

async fn handle_healthz(
    State(state): State<SupervisorHttpState>,
) -> (StatusCode, Json<HealthzBody>) {
    (StatusCode::OK, Json(state.healthz))
}

async fn handle_readyz(
    State(state): State<SupervisorHttpState>,
) -> (StatusCode, Json<ReadyzResponseBody>) {
    let summary = state.shared.ready_summary();
    let code = if summary.status == "ready" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(summary))
}

async fn handle_metrics(State(state): State<SupervisorHttpState>) -> (StatusCode, String) {
    let summary = state.shared.ready_summary();
    let ready_gauge = if summary.status == "ready" { 1.0 } else { 0.0 };
    metrics::gauge!(
        "zinder_build_info",
        "service" => "zinder",
        "version" => env!("CARGO_PKG_VERSION"),
        "git_commit" => BUILD_GIT_COMMIT,
        "network" => summary.network
    )
    .set(1.0);
    metrics::gauge!(
        "zinder_supervisor_ready",
        "mode" => summary.mode,
        "preset" => summary.preset,
        "network" => summary.network
    )
    .set(ready_gauge);
    for role_snap in &summary.roles {
        metrics::gauge!(
            "zinder_supervisor_role_ready",
            "role" => role_snap.role,
            "network" => summary.network
        )
        .set(if role_snap.ready { 1.0 } else { 0.0 });
    }

    let local_metrics = state.metrics.render();
    let targets = state.shared.scrape_targets();
    let combined = aggregate_prometheus_metrics(&local_metrics, &targets).await;
    (StatusCode::OK, combined)
}

async fn aggregate_prometheus_metrics(
    local_metrics: &str,
    targets: &[(ServiceRole, SocketAddr)],
) -> String {
    let mut output = String::with_capacity(local_metrics.len() + targets.len() * 2048);
    let mut seen_headers = HashSet::new();
    append_prometheus_section(&mut output, local_metrics, &mut seen_headers);

    for &(role, addr) in targets {
        if let Ok((StatusCode::OK, body)) = http_get_localhost(addr, "/metrics").await {
            let _ = writeln!(output, "# child_role: {}", role.label());
            append_prometheus_section(&mut output, &body, &mut seen_headers);
        }
    }
    output
}

fn append_prometheus_section(
    output: &mut String,
    rendered: &str,
    seen_headers: &mut HashSet<String>,
) {
    for line in rendered.lines() {
        if (line.starts_with("# HELP ") || line.starts_with("# TYPE "))
            && !seen_headers.insert(line.to_owned())
        {
            continue;
        }
        output.push_str(line);
        output.push('\n');
    }
}

/// Fetches a child operational `/readyz` endpoint over loopback HTTP/1.1.
pub async fn fetch_child_readyz(addr: SocketAddr) -> Option<(bool, ChildReadyzPayload)> {
    let (status, body) = http_get_localhost(addr, "/readyz").await.ok()?;
    let payload = serde_json::from_str::<ChildReadyzPayload>(&body).ok()?;
    Some((status == StatusCode::OK, payload))
}

async fn http_get_localhost(
    addr: SocketAddr,
    path: &str,
) -> Result<(StatusCode, String), &'static str> {
    tokio::time::timeout(CHILD_SCRAPE_TIMEOUT, http_get_inner(addr, path))
        .await
        .map_err(|_| "scrape timed out")?
}

async fn http_get_inner(
    addr: SocketAddr,
    path: &str,
) -> Result<(StatusCode, String), &'static str> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|_| "connect failed")?;
    let io = TokioIo::new(stream);
    let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|_| "handshake failed")?;
    let conn_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    let request = Request::builder()
        .method("GET")
        .uri(path)
        .header("Host", addr.to_string())
        .header("Connection", "close")
        .body(Empty::<Bytes>::new())
        .map_err(|_| "request build failed")?;

    let response = sender
        .send_request(request)
        .await
        .map_err(|_| "send request failed")?;
    let status = response.status();
    let collected = response
        .into_body()
        .collect()
        .await
        .map_err(|_| "body collect failed")?
        .to_bytes();
    conn_task.abort();
    let text = String::from_utf8_lossy(&collected).into_owned();
    Ok((status, text))
}
